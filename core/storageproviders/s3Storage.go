package storageproviders

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/grafov/m3u8"
	"github.com/owncast/owncast/config"
	"github.com/owncast/owncast/core/data"
	"github.com/owncast/owncast/core/playlist"
	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// can use rewriteRemotePlaylist to make the URLs absolute,
// but shouldn't do this for the master playlist because that needs to be relative to get to the varient playlists
// rewrite the varient playlists
// do not upload the playlists to s3

// If we try to upload a playlist but it is not yet on disk
// then keep a reference to it here.
var _queuedPlaylistUpdates = make(map[string]string)

// S3Storage is the s3 implementation of the ChunkStorageProvider.
type S3Storage struct {
	sess *session.Session
	host string

	s3Endpoint        string
	s3ServingEndpoint string
	s3Region          string
	s3Bucket          string
	s3AccessKey       string
	s3Secret          string
	s3ACL             string
}

var _uploader *s3manager.Uploader

// Setup sets up the s3 storage for saving the video to s3.
func (s *S3Storage) Setup() error {
	log.Trace("Setting up S3 for external storage of video...")

	s3Config := data.GetS3Config()
	if s3Config.ServingEndpoint != "" {
		s.host = s3Config.ServingEndpoint
	} else {
		s.host = fmt.Sprintf("%s/%s", s3Config.Endpoint, s3Config.Bucket)
	}

	s.s3Endpoint = s3Config.Endpoint
	s.s3ServingEndpoint = s3Config.ServingEndpoint
	s.s3Region = s3Config.Region
	s.s3Bucket = s3Config.Bucket
	s.s3AccessKey = s3Config.AccessKey
	s.s3Secret = s3Config.Secret
	s.s3ACL = s3Config.ACL

	s.sess = s.connectAWS()

	_uploader = s3manager.NewUploader(s.sess)

	return nil
}

// SegmentWritten is called when a single segment of video is written.
func (s *S3Storage) SegmentWritten(localFilePath string) {
	//	index := utils.GetIndexFromFilePath(localFilePath)

	playlistPath := filepath.Join(filepath.Dir(localFilePath), "stream.m3u8")
	// NIKKI: rewrite the variant playlist to have absolute urls
	if err := s.rewriteRemotePlaylist(playlistPath); err != nil {
		log.Warnln(err)
	}

	// Upload the variant playlist for this segment
	// so the segments and the HLS playlist referencing
	// them are in sync.
	// playlistPath := filepath.Join(filepath.Dir(localFilePath), "stream.m3u8")
	/*if _, err := s.Save(playlistPath, 0); err != nil {
		_queuedPlaylistUpdates[playlistPath] = playlistPath
		if pErr, ok := err.(*os.PathError); ok {
			log.Debugln(pErr.Path, "does not yet exist locally when trying to upload to S3 storage.")
			return
		}
	}*/
}

// VariantPlaylistWritten is called when a variant hls playlist is written.
func (s *S3Storage) VariantPlaylistWritten(localFilePath string) {
	// We are uploading the variant playlist after uploading the segment
	// to make sure we're not referring to files in a playlist that don't
	// yet exist.  See SegmentWritten.

	// NIKKI: rewrite the variant playlist to have full urls, do NOT save it to s3
	//if err := s.rewriteRemotePlaylist(localFilePath); err != nil {
	//	log.Warnln(err)
	//}

	/*if _, ok := _queuedPlaylistUpdates[localFilePath]; ok {
		if _, err := s.Save(localFilePath, 0); err != nil {
			log.Errorln(err)
			_queuedPlaylistUpdates[localFilePath] = localFilePath
		}
		delete(_queuedPlaylistUpdates, localFilePath)
	}*/
}

// MasterPlaylistWritten is called when the master hls playlist is written.
func (s *S3Storage) MasterPlaylistWritten(localFilePath string) {
	// Rewrite the playlist to use absolute remote S3 URLs
	// NIKKI: copy the playlist into the public path
	//if err := s.rewriteRemotePlaylist(localFilePath); err != nil {
	//	log.Warnln(err)
	//}
	publicPath := filepath.Join(config.PublicHLSStoragePath, filepath.Base(localFilePath))
	data, err := ioutil.ReadFile(localFilePath)
	if err != nil {
		panic(err)
	}
	_ = ioutil.WriteFile(publicPath, data, 0644)
}

// Save saves the file to the s3 bucket.
// NIKKI: not any more. Using a CDN that grabs the video from the server instead.
// NIKKI: make nginx serve any .ts file from the hls directory, the playlists from the webroot/hls directory
func (s *S3Storage) Save(filePath string, retryCount int) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	return filePath, nil
	/*

		maxAgeSeconds := utils.GetCacheDurationSecondsForPath(filePath)
		cacheControlHeader := fmt.Sprintf("max-age=%d", maxAgeSeconds)
		uploadInput := &s3manager.UploadInput{
			Bucket:       aws.String(s.s3Bucket), // Bucket to be used
			Key:          aws.String(filePath),   // Name of the file to be saved
			Body:         file,                   // File
			CacheControl: &cacheControlHeader,
		}

		if s.s3ACL != "" {
			uploadInput.ACL = aws.String(s.s3ACL)
		} else {
			// Default ACL
			uploadInput.ACL = aws.String("public-read")
		}

		response, err := _uploader.Upload(uploadInput)

		if err != nil {
			log.Traceln("error uploading:", filePath, err.Error())
			if retryCount < 4 {
				log.Traceln("Retrying...")
				return s.Save(filePath, retryCount+1)
			} else {
				log.Warnln("Giving up on", filePath, err)
				return "", fmt.Errorf("Giving up on %s", filePath)
			}
		}

		return response.Location, nil*/
}

func (s *S3Storage) connectAWS() *session.Session {
	creds := credentials.NewStaticCredentials(s.s3AccessKey, s.s3Secret, "")
	_, err := creds.Get()
	if err != nil {
		log.Panicln(err)
	}

	sess, err := session.NewSession(
		&aws.Config{
			Region:      aws.String(s.s3Region),
			Credentials: creds,
			Endpoint:    aws.String(s.s3Endpoint),
		},
	)

	if err != nil {
		log.Panicln(err)
	}
	return sess
}

// rewriteRemotePlaylist will take a local playlist and rewrite it to have absolute URLs to remote locations.
func (s *S3Storage) rewriteRemotePlaylist(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	p, _, err := m3u8.DecodeFrom(bufio.NewReader(f), true)
	if err != nil {
		log.Fatalln(err)
	}
	variantPlaylist := p.(*m3u8.MediaPlaylist)
	streamTrack := filepath.Base(filepath.Dir(filePath))
	for _, item := range variantPlaylist.Segments {
		if item != nil {
			item.URI = s.host + filepath.Join("/hls", streamTrack, item.URI)
		}
	}
	publicPath := filepath.Join(config.PublicHLSStoragePath, streamTrack, filepath.Base(filePath))
	newPlaylist := variantPlaylist.String()
	return playlist.WritePlaylist(newPlaylist, publicPath)
}
