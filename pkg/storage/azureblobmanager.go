package storage

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
)

type AzureBlobManager struct {
	client               *azblob.Client
	containerName        string
	sharedKeyCredentials *azblob.SharedKeyCredential
	signedURLExpiry      time.Duration
}

func (manager *AzureBlobManager) BlobExists(ctx context.Context, key string) (bool, error) {
	_, err := manager.
		client.ServiceClient().
		NewContainerClient(manager.containerName).
		NewBlobClient(key).
		GetProperties(ctx, nil)

	if err == nil {
		return true, nil
	}

	if bloberror.HasCode(err, bloberror.ContainerNotFound, bloberror.BlobNotFound) {
		return false, nil
	}

	return false, err
}

func (manager *AzureBlobManager) GetPresignedURL(ctx context.Context, path string) (string, error) {
	var queryParam sas.QueryParameters
	var udc *service.UserDelegationCredential
	var err error
	// because of clock skew it can happen that the token is not yet valid, so make it valid in the past
	startTime := time.Now().Add(-10 * time.Minute).UTC()
	expiryTime := time.Now().Add(manager.signedURLExpiry).UTC()
	blobSignatureValues := sas.BlobSignatureValues{
		ContainerName: manager.containerName,
		BlobName:      path,
		Protocol:      sas.ProtocolHTTPS,
		StartTime:     startTime,
		ExpiryTime:    expiryTime,
		Permissions:   to.Ptr(sas.BlobPermissions{Read: true}).String(),
	}

	if manager.sharedKeyCredentials == nil {
		info := service.KeyInfo{
			Start:  to.Ptr(startTime.Format(sas.TimeFormat)),
			Expiry: to.Ptr(expiryTime.Format(sas.TimeFormat)),
		}
		udc, err = manager.client.ServiceClient().GetUserDelegationCredential(ctx, info, nil)
		if err != nil {
			return "", err
		}

		queryParam, err = blobSignatureValues.SignWithUserDelegation(udc)
	} else {
		queryParam, err = blobSignatureValues.SignWithSharedKey(manager.sharedKeyCredentials)
	}
	if err != nil {
		return "", err
	}

	url := fmt.Sprintf("%s?%s", manager.client.ServiceClient().NewContainerClient(manager.containerName).NewBlobClient(path).URL(), queryParam.Encode())
	return url, nil
}

func (manager *AzureBlobManager) PutBlob(ctx context.Context, key string, body io.Reader) error {
	_, err := manager.client.UploadStream(ctx, manager.containerName, key, body, nil)
	return err
}

func (manager *AzureBlobManager) GetBlob(ctx context.Context, key string) ([]byte, error) {
	var buffer []byte

	_, err := manager.client.DownloadBuffer(ctx, manager.containerName, key, buffer, nil)
	if err != nil {
		return nil, err
	}

	return buffer, nil
}

func (manager *AzureBlobManager) ListBlobs(ctx context.Context, prefix string) ([]string, error) {
	pager := manager.client.ServiceClient().
		NewContainerClient(manager.containerName).
		NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
			Prefix: to.Ptr(prefix),
		})

	blobs := make([]string, 0)

	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, item := range page.Segment.BlobItems {
			blobs = append(blobs, *item.Name)
		}
	}

	return blobs, nil
}

func NewAzureBlobManager(config *AzureStorage, blobClient *azblob.Client) *AzureBlobManager {
	return &AzureBlobManager{
		containerName:        config.containerName,
		client:               blobClient,
		sharedKeyCredentials: config.sharedKeyCredentials,
		signedURLExpiry:      config.signedURLExpiry,
	}
}
