package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"time"

	"github.com/boring-registry/boring-registry/pkg/core"
	"github.com/boring-registry/boring-registry/pkg/module"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
)

type azureBlobManager interface {
	BlobExists(ctx context.Context, key string) (bool, error)
	GetPresignedURL(ctx context.Context, path string) (string, error)
	PutBlob(ctx context.Context, key string, body io.Reader) error
	GetBlob(ctx context.Context, key string) ([]byte, error)
	ListBlobs(ctx context.Context, prefix string) ([]string, error)
}

// AzureStorage is a Storage implementation backed by Azure Storage Account.
// AzureStorage implements module.Storage and provider.Storage
type AzureStorage struct {
	blobManager          azureBlobManager
	sharedKeyCredentials *azblob.SharedKeyCredential
	accountName          string
	containerName        string
	prefix               string
	moduleArchiveFormat  string
	accountKey           string
	accountEndpoint      string
	signedURLExpiry      time.Duration
}

// GetModule retrieves information about a module from the Azure storage.
func (s *AzureStorage) GetModule(ctx context.Context, namespace, name, provider, version string) (core.Module, error) {
	key := modulePath(s.prefix, namespace, name, provider, version, s.moduleArchiveFormat)

	exists, err := s.blobManager.BlobExists(ctx, key)
	if err != nil {
		return core.Module{}, err
	} else if !exists {
		return core.Module{}, module.ErrModuleNotFound
	}

	presigned, err := s.blobManager.GetPresignedURL(ctx, key)
	if err != nil {
		return core.Module{}, err
	}

	return core.Module{
		Namespace:   namespace,
		Name:        name,
		Provider:    provider,
		Version:     version,
		DownloadURL: presigned,
	}, nil
}

func (s *AzureStorage) ListModuleVersions(ctx context.Context, namespace, name, provider string) ([]core.Module, error) {
	prefix := modulePathPrefix(s.prefix, namespace, name, provider)
	blobs, err := s.blobManager.ListBlobs(ctx, prefix)
	if err != nil {
		return []core.Module{}, err
	}

	var modules []core.Module
	for _, blob := range blobs {
		m, err := moduleFromObject(blob, s.moduleArchiveFormat)
		if err != nil {
			// TODO: we're skipping possible failures silently
			continue
		}

		// The download URL is probably not necessary for ListModules
		m.DownloadURL, err = s.blobManager.GetPresignedURL(ctx, modulePath(s.prefix, m.Namespace, m.Name, m.Provider, m.Version, s.moduleArchiveFormat))
		if err != nil {
			return []core.Module{}, err
		}

		modules = append(modules, *m)
	}

	return modules, nil
}

// UploadModule uploads a module to the S3 storage.
func (s *AzureStorage) UploadModule(ctx context.Context, namespace, name, provider, version string, body io.Reader) (core.Module, error) {
	if namespace == "" {
		return core.Module{}, errors.New("namespace not defined")
	}

	if name == "" {
		return core.Module{}, errors.New("name not defined")
	}

	if provider == "" {
		return core.Module{}, errors.New("provider not defined")
	}

	if version == "" {
		return core.Module{}, errors.New("version not defined")
	}

	key := modulePath(s.prefix, namespace, name, provider, version, DefaultModuleArchiveFormat)

	if _, err := s.GetModule(ctx, namespace, name, provider, version); err == nil {
		return core.Module{}, fmt.Errorf("%w: %s", module.ErrModuleAlreadyExists, key)
	}

	if err := s.blobManager.PutBlob(ctx, key, body); err != nil {
		return core.Module{}, fmt.Errorf("%v: %w", module.ErrModuleUploadFailed, err)
	}

	return s.GetModule(ctx, namespace, name, provider, version)
}

// GetProvider retrieves information about a provider from the Azure storage.
func (s *AzureStorage) getProvider(ctx context.Context, pt providerType, provider *core.Provider) (*core.Provider, error) {
	var archivePath, shasumPath, shasumSigPath string
	if pt == internalProviderType {
		archivePath, shasumPath, shasumSigPath = internalProviderPath(s.prefix, provider.Namespace, provider.Name, provider.Version, provider.OS, provider.Arch)
	} else if pt == mirrorProviderType {
		archivePath, shasumPath, shasumSigPath = mirrorProviderPath(s.prefix, provider.Hostname, provider.Namespace, provider.Name, provider.Version, provider.OS, provider.Arch)
	}

	if exists, err := s.blobManager.BlobExists(ctx, archivePath); err != nil {
		return nil, err
	} else if !exists {
		return nil, noMatchingProviderFound(provider)
	}

	var err error
	provider.DownloadURL, err = s.blobManager.GetPresignedURL(ctx, archivePath)
	if err != nil {
		return nil, err
	}
	provider.SHASumsURL, err = s.blobManager.GetPresignedURL(ctx, shasumPath)
	if err != nil {
		return nil, fmt.Errorf("failed to generate presigned url for %s: %w", shasumPath, err)
	}
	provider.SHASumsSignatureURL, err = s.blobManager.GetPresignedURL(ctx, shasumSigPath)
	if err != nil {
		return nil, err
	}

	shasumBytes, err := s.blobManager.GetBlob(ctx, shasumPath)
	if err != nil {
		return nil, err
	}

	provider.Shasum, err = readSHASums(bytes.NewReader(shasumBytes), path.Base(archivePath))
	if err != nil {
		return nil, err
	}

	var signingKeys *core.SigningKeys
	if pt == internalProviderType {
		signingKeys, err = s.SigningKeys(ctx, provider.Namespace)
	} else if pt == mirrorProviderType {
		signingKeys, err = s.MirroredSigningKeys(ctx, provider.Hostname, provider.Namespace)
	}
	if err != nil {
		return nil, err
	}

	provider.Filename = path.Base(archivePath)
	provider.SigningKeys = *signingKeys
	return provider, nil
}

func (s *AzureStorage) GetProvider(ctx context.Context, namespace, name, version, os, arch string) (*core.Provider, error) {
	p, err := s.getProvider(ctx, internalProviderType, &core.Provider{
		Namespace: namespace,
		Name:      name,
		Version:   version,
		OS:        os,
		Arch:      arch,
	})

	return p, err
}

func (s *AzureStorage) GetMirroredProvider(ctx context.Context, provider *core.Provider) (*core.Provider, error) {
	return s.getProvider(ctx, mirrorProviderType, provider)
}

func (s *AzureStorage) listProviderVersions(ctx context.Context, pt providerType, provider *core.Provider) ([]*core.Provider, error) {
	prefix := providerStoragePrefix(s.prefix, pt, provider.Hostname, provider.Namespace, provider.Name)
	blobs, err := s.blobManager.ListBlobs(ctx, prefix)
	if err != nil {
		return nil, noMatchingProviderFound(provider)
	}

	var providers []*core.Provider
	for _, blob := range blobs {
		p, err := core.NewProviderFromArchive(filepath.Base(blob))
		if err != nil {
			continue
		}

		if provider.Version != "" && provider.Version != p.Version {
			// The provider version doesn't match the requested version
			continue
		}

		p.Hostname = provider.Hostname
		p.Namespace = provider.Namespace
		archiveUrl, err := s.blobManager.GetPresignedURL(ctx, blob)

		if err != nil {
			return nil, err
		}
		p.DownloadURL = archiveUrl

		providers = append(providers, &p)
	}

	if len(providers) == 0 {
		return nil, noMatchingProviderFound(provider)
	}

	return providers, nil
}

func (s *AzureStorage) ListProviderVersions(ctx context.Context, namespace, name string) (*core.ProviderVersions, error) {
	providers, err := s.listProviderVersions(ctx, internalProviderType, &core.Provider{Namespace: namespace, Name: name})
	if err != nil {
		return nil, err
	}

	collection := NewCollection()
	for _, p := range providers {
		collection.Add(p)
	}
	return collection.List(), nil
}

func (s *AzureStorage) ListMirroredProviders(ctx context.Context, provider *core.Provider) ([]*core.Provider, error) {
	return s.listProviderVersions(ctx, mirrorProviderType, provider)
}

func (s *AzureStorage) UploadProviderReleaseFiles(ctx context.Context, namespace, name, filename string, file io.Reader) error {
	if namespace == "" {
		return fmt.Errorf("namespace argument is empty")
	}

	if name == "" {
		return fmt.Errorf("name argument is empty")
	}

	if filename == "" {
		return fmt.Errorf("name argument is empty")
	}

	prefix := providerStoragePrefix(s.prefix, internalProviderType, "", namespace, name)
	key := filepath.Join(prefix, filename)
	return s.upload(ctx, key, file, false)
}

func (s *AzureStorage) signingKeys(ctx context.Context, pt providerType, hostname, namespace string) (*core.SigningKeys, error) {
	if namespace == "" {
		return nil, fmt.Errorf("namespace argument is empty")
	}
	key := signingKeysPath(s.prefix, pt, hostname, namespace)
	exists, err := s.blobManager.BlobExists(ctx, key)
	if err != nil {
		return nil, err
	} else if !exists {
		return nil, core.ErrObjectNotFound
	}

	signingKeysRaw, err := s.blobManager.GetBlob(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to download signing_keys.json for namespace %s: %w", namespace, err)
	}

	return unmarshalSigningKeys(signingKeysRaw)
}

// SigningKeys downloads the JSON placed in the namespace in Azure Storage and unmarshals it into a core.SigningKeys
func (s *AzureStorage) SigningKeys(ctx context.Context, namespace string) (*core.SigningKeys, error) {
	return s.signingKeys(ctx, internalProviderType, "", namespace)
}

func (s *AzureStorage) MirroredSigningKeys(ctx context.Context, hostname, namespace string) (*core.SigningKeys, error) {
	return s.signingKeys(ctx, mirrorProviderType, hostname, namespace)
}

func (s *AzureStorage) MirroredSha256Sum(ctx context.Context, provider *core.Provider) (*core.Sha256Sums, error) {
	prefix := providerStoragePrefix(s.prefix, mirrorProviderType, provider.Hostname, provider.Namespace, provider.Name)
	key := filepath.Join(prefix, provider.ShasumFileName())
	shaSumBytes, err := s.blobManager.GetBlob(ctx, key)
	if err != nil {
		return nil, errors.New("failed to download SHA256SUMS")
	}

	return core.NewSha256Sums(provider.ShasumFileName(), bytes.NewReader(shaSumBytes))
}

func (s *AzureStorage) uploadSigningKeys(ctx context.Context, pt providerType, hostname, namespace string, signingKeys *core.SigningKeys) error {
	b, err := json.Marshal(signingKeys)
	if err != nil {
		return err
	}
	key := signingKeysPath(s.prefix, pt, hostname, namespace)
	return s.upload(ctx, key, bytes.NewReader(b), true)
}

func (s *AzureStorage) UploadMirroredSigningKeys(ctx context.Context, hostname, namespace string, signingKeys *core.SigningKeys) error {
	return s.uploadSigningKeys(ctx, mirrorProviderType, hostname, namespace, signingKeys)
}

func (s *AzureStorage) UploadMirroredFile(ctx context.Context, provider *core.Provider, fileName string, reader io.Reader) error {
	prefix := providerStoragePrefix(s.prefix, mirrorProviderType, provider.Hostname, provider.Namespace, provider.Name)
	key := filepath.Join(prefix, fileName)
	return s.upload(ctx, key, reader, true)
}

func (s *AzureStorage) upload(ctx context.Context, key string, reader io.Reader, overwrite bool) error {
	// If we don't want to overwrite, check if the object exists
	if !overwrite {
		exists, err := s.blobManager.BlobExists(ctx, key)
		if err != nil {
			return err
		} else if exists {
			return fmt.Errorf("failed to upload key %s: %w", key, core.ErrObjectAlreadyExists)
		}
	}

	if err := s.blobManager.PutBlob(ctx, key, reader); err != nil {
		return fmt.Errorf("failed to upload: %w", err)
	}

	return nil
}

// AzureStorageOption provides additional options for the AzureStorage.
type AzureStorageOption func(*AzureStorage)

// WithAzureStorageSignedUrlExpiry configures the duration until the signed url expires
func WithAzureStorageSignedUrlExpiry(t time.Duration) AzureStorageOption {
	return func(s *AzureStorage) {
		s.signedURLExpiry = t
	}
}

// WithAzureStorageAccountKey configures the account access key to use for authentication
func WithAzureStorageAccountKey(accountKey string) AzureStorageOption {
	return func(s *AzureStorage) {
		s.accountKey = accountKey
	}
}

// WithAzureStorageAccountEndpoint configures the fully-qualified account endpoint (for local development or non-standard endpoints)
func WithAzureStorageAccountEndpoint(endpoint string) AzureStorageOption {
	return func(s *AzureStorage) {
		s.accountEndpoint = endpoint
	}
}

// WithAzureStoragePrefix configures the blob prefix path within a storage container
func WithAzureStoragePrefix(prefix string) AzureStorageOption {
	return func(s *AzureStorage) {
		s.prefix = prefix
	}
}

// WithAzureArchiveFormat configures the module archive format (zip, tar, tgz, etc.)
func WithAzureArchiveFormat(archiveFormat string) AzureStorageOption {
	return func(s *AzureStorage) {
		s.moduleArchiveFormat = archiveFormat
	}
}

// NewAzureStorage returns a fully initialized Azure storage.
func NewAzureStorage(ctx context.Context, accountName string, containerName string, options ...AzureStorageOption) (Storage, error) {
	// Required- and default-values should be set here
	s := &AzureStorage{
		accountName:   accountName,
		containerName: containerName,
	}

	for _, option := range options {
		option(s)
	}

	serviceURL := s.accountEndpoint
	if serviceURL == "" {
		serviceURL = fmt.Sprintf("https://%s.blob.core.windows.net", s.accountName)
	}

	var client *azblob.Client
	if s.accountKey == "" {
		defaultCreds, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, err
		}
		client, err = azblob.NewClient(serviceURL, defaultCreds, nil)
		if err != nil {
			return nil, err
		}
	} else {
		sasCredentials, err := blob.NewSharedKeyCredential(s.accountName, s.accountKey)
		if err != nil {
			return nil, err
		}
		client, err = azblob.NewClientWithSharedKeyCredential(serviceURL, sasCredentials, nil)
		if err != nil {
			return nil, err
		}
		s.sharedKeyCredentials = sasCredentials
	}

	s.blobManager = NewAzureBlobManager(s, client)
	return s, nil
}
