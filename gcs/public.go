package gcs

import (
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	gs "cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/smira/aptly/aptly"
	"github.com/smira/aptly/utils"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

// PublishedStorage abstract file system with published files (actually hosted on GCS)
type PublishedStorage struct {
	client     *gs.Client
	bucketName string
	prefix     string
	pathCache  map[string]string
	acl        string
}

// NewPublishedStorage creates a new instance of PublishedStorage
func NewPublishedStorage(bucketName string, prefix string, acl string) (*PublishedStorage, error) {
	client, err := gs.NewClient(context.Background())
	if err != nil {
		log.Fatalf("Unable to create storage service: %v", err)
	}

	result := &PublishedStorage{
		client:     client,
		bucketName: bucketName,
		prefix:     prefix,
		acl:        acl,
	}
	return result, nil
}

// String returns the stroage information
func (storage *PublishedStorage) String() string {
	return fmt.Sprintf("GCS: %s:%s", storage.bucketName, storage.prefix)
}

// MkDir creates directory recursively under public path
func (storage *PublishedStorage) MkDir(path string) error {
	// noop - GCS does not have <airquotes> directories </airquotes>
	return nil
}

// PutFile puts file into published storage at specified path
func (storage *PublishedStorage) PutFile(path string, sourceFilename string) error {
	f, err := os.Open(sourceFilename)
	if err != nil {
		return err
	}
	defer f.Close()

	return storage.putFile(path, f)
}

func (storage *PublishedStorage) putFile(path string, source io.Reader) error {
	ctx := context.Background()
	w := storage.client.Bucket(storage.bucketName).Object(filepath.Join(storage.prefix, path)).NewWriter(ctx)

	if storage.acl == "public-read" {
		w.ACL = []gs.ACLRule{
			gs.ACLRule{
				Entity: gs.AllUsers,
				Role:   gs.RoleReader,
			},
		}
	}

	_, err := io.Copy(w, source)
	if err != nil {
		return err
	}

	return w.Close()
}

// Remove removes single file under public path
func (storage *PublishedStorage) Remove(path string) error {
	realPath := filepath.Join(storage.prefix, path)
	return storage.client.Bucket(storage.bucketName).Object(realPath).Delete(context.Background())
}

// RemoveDirs removes directory structure under public path
func (storage *PublishedStorage) RemoveDirs(path string, progress aptly.Progress) error {
	filelist, err := storage.Filelist(path)
	if err != nil {
		return err
	}

	// progress.InitBar(int64(len(filelist)), false)
	// defer progress.ShutdownBar()

	// remove all files in filelist. Is the directory itself deleted?
	for _, fn := range filelist {
		// Don't delete everything by accident
		objName := filepath.Join(storage.prefix, path, fn)
		err = storage.client.Bucket(storage.bucketName).Object(objName).Delete(context.Background())
		if err != nil {
			return err
		}
	}

	return nil
}

// LinkFromPool links package file from pool to dist's pool location
//
// publishedDirectory is desired location in pool (like prefix/pool/component/liba/libav/)
// sourcePool is instance of aptly.PackagePool
// sourcePath is filepath to package file in package pool
//
// LinkFromPool returns relative path for the published file to be included in package index
func (storage *PublishedStorage) LinkFromPool(
	publishedDirectory, baseName string,
	sourcePool aptly.PackagePool,
	sourcePath string,
	sourceChecksums utils.ChecksumInfo,
	force bool,
) error {
	relPath := filepath.Join(publishedDirectory, baseName)
	poolPath := filepath.Join(storage.prefix, relPath)

	if storage.pathCache == nil {
		paths, md5s, err := storage.internalFilelist("")
		if err != nil {
			return errors.Wrap(err, "error caching paths under prefix")
		}

		storage.pathCache = make(map[string]string, len(paths))
		for k, path := range paths {
			storage.pathCache[path] = md5s[k]
		}
	}

	destMD5, exists := storage.pathCache[relPath]
	sourceMD5 := sourceChecksums.MD5

	if exists {
		if sourceMD5 == "" {
			return fmt.Errorf("unbale to compre object, MD5 checksum missing")
		}

		if destMD5 == sourceMD5 {
			return nil
		}

		if !force && destMD5 != sourceMD5 {
			return fmt.Errorf("error putting file to %s: file already exists and is different: %s", poolPath, storage)
		}
	}

	source, err := sourcePool.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()

	err = storage.putFile(relPath, source)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error uploading %s to %s: %s", sourcePath, storage, poolPath))
	}

	storage.pathCache[relPath] = sourceMD5

	return nil
}

// Filelist returns list of files under prefix
func (storage *PublishedStorage) Filelist(prefix string) ([]string, error) {
	paths, _, err := storage.internalFilelist(prefix)
	return paths, err
}

func (storage *PublishedStorage) internalFilelist(path string) ([]string, []string, error) {
	paths := []string{}
	md5s := []string{}

	prefix := filepath.Join(storage.prefix, path)
	if prefix != "" {
		prefix += "/"
	}

	q := &gs.Query{Prefix: prefix}
	it := storage.client.Bucket(storage.bucketName).Objects(context.Background(), q)
	for {
		objAttrs, err := it.Next()
		if err != nil && err != iterator.Done {
			return nil, nil, err
		}
		if err == iterator.Done {
			break
		}

		fn := objAttrs.Name
		if prefix != "" {
			fn = fn[len(prefix):]
		}

		paths = append(paths, fn)
		md5s = append(md5s, hex.EncodeToString(objAttrs.MD5))
	}

	return paths, md5s, nil
}

// RenameFile renames (moves) file
func (storage *PublishedStorage) RenameFile(oldName, newName string) error {
	srcPath := filepath.Join(storage.prefix, oldName)
	srcObj := storage.client.Bucket(storage.bucketName).Object(srcPath)
	destPath := filepath.Join(storage.prefix, newName)
	destObj := storage.client.Bucket(storage.bucketName).Object(destPath)

	_, err := destObj.CopierFrom(srcObj).Run(context.Background())
	if err != nil {
		return err
	}

	return srcObj.Delete(context.Background())
}
