package storageproviders

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/grafov/m3u8"
	log "github.com/sirupsen/logrus"

	"github.com/owncast/owncast/config"
	"github.com/owncast/owncast/core/data"
	"github.com/owncast/owncast/core/playlist"
	"github.com/owncast/owncast/core/transcoder"
	"github.com/owncast/owncast/utils"
)

type LocalStorage struct {
	cdnHost string
}

// Cleanup old public HLS content every N min from the webroot.
var _onlineCleanupTicker *time.Ticker

// Setup configures this storage provider.
func (s *LocalStorage) Setup() error {
	// NOTE: This cleanup timer will have to be disabled to support recordings in the future
	// as all HLS segments have to be publicly available on disk to keep a recording of them.
	_onlineCleanupTicker = time.NewTicker(1 * time.Minute)
	s3Config := data.GetS3Config()
	if s3Config.ServingEndpoint != "" {
		s.cdnHost = s3Config.ServingEndpoint
	}
	go func() {
		for range _onlineCleanupTicker.C {
			transcoder.CleanupOldContent(config.PublicHLSStoragePath)
		}
	}()
	return nil
}

// SegmentWritten is called when a single segment of video is written.
func (s *LocalStorage) SegmentWritten(localFilePath string) {
	if _, err := s.Save(localFilePath, 0); err != nil {
		log.Warnln(err)
	}
}

// VariantPlaylistWritten is called when a variant hls playlist is written.
func (s *LocalStorage) VariantPlaylistWritten(localFilePath string) {
	if _, err := s.Save(localFilePath, 0); err != nil {
		log.Errorln(err)
		return
	}
}

// MasterPlaylistWritten is called when the master hls playlist is written.
func (s *LocalStorage) MasterPlaylistWritten(localFilePath string) {
	if _, err := s.Save(localFilePath, 0); err != nil {
		log.Warnln(err)
	}
}

// Save will save a local filepath using the storage provider.
func (s *LocalStorage) Save(filePath string, retryCount int) (string, error) {
	newPath := ""

	// This is a hack
	if filePath == "hls/stream.m3u8" {
		newPath = filepath.Join(config.PublicHLSStoragePath, filepath.Base(filePath))
	} else if strings.HasSuffix(filePath, "stream.m3u8") {
		newPath = filepath.Join(config.WebRoot, filePath)
		err := s.addCDN(filePath)
		if err != nil {
			return newPath, err
		}
	} else {
		newPath = filepath.Join(config.WebRoot, filePath)
	}

	err := utils.Copy(filePath, newPath)
	return newPath, err
}

func (s *LocalStorage) addCDN(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	p, _, err := m3u8.DecodeFrom(bufio.NewReader(f), true)
	f.Close()

	if err != nil {
		log.Fatalln(err)
	}

	variantPlaylist := p.(*m3u8.MediaPlaylist)
	streamTrack := filepath.Base(filepath.Dir(filePath))
	for _, item := range variantPlaylist.Segments {
		if item != nil {
			item.URI = s.cdnHost + filepath.Join("/hls", streamTrack, item.URI)
		}
	}

	newPlaylist := variantPlaylist.String()
	return playlist.WritePlaylist(newPlaylist, filePath)
}
