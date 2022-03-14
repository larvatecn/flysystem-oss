<?php
/**
 * This is NOT a freeware, use is subject to license terms
 * @copyright Copyright (c) 2010-2099 Jinan Larva Information Technology Co., Ltd.
 * @link http://www.larva.com.cn/
 */

namespace Larva\Flysystem\Oss;

use Carbon\Carbon;
use League\Flysystem\Config;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\FileAttributes;
use League\Flysystem\FilesystemAdapter;
use League\Flysystem\FilesystemException;
use League\Flysystem\PathPrefixer;
use League\Flysystem\StorageAttributes;
use League\Flysystem\UnableToCheckExistence;
use League\Flysystem\UnableToCopyFile;
use League\Flysystem\UnableToCreateDirectory;
use League\Flysystem\UnableToDeleteDirectory;
use League\Flysystem\UnableToDeleteFile;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToRetrieveMetadata;
use League\Flysystem\UnableToSetVisibility;
use League\Flysystem\UnableToWriteFile;
use OSS\Core\OssException;
use OSS\OssClient;

/**
 * 阿里云适配器
 */
class OSSAdapter implements FilesystemAdapter
{
    /**
     * @var OssClient
     */
    protected OssClient $client;

    /**
     * @var array
     */
    protected array $config = [];

    protected PathPrefixer $prefixer;

    /**
     * 导出扩展 MetaData 字段
     * @var string[]
     */
    private const EXTRA_METADATA_FIELDS = [
        'x-oss-object-type',
        'x-oss-storage-class',
        'x-oss-hash-crc64ecma',
        'etag',
        'content-md5',
    ];

    /**
     * Adapter constructor.
     *
     * @param array $config
     * @throws OssException
     */
    public function __construct(array $config)
    {
        $this->config = $config;
        $this->client = new OssClient($config['access_id'], $config['access_key'], $config['endpoint'], false, $config['security_token'] ?? null, $config['proxy'] ?? null);
        $this->prefixer = new PathPrefixer($config['prefix'] ?? '', DIRECTORY_SEPARATOR);
    }

    /**
     * 判断文件是否存在
     * @param string $path
     * @return bool
     * @throws UnableToCheckExistence
     */
    public function fileExists(string $path): bool
    {
        try {
            return $this->fetchFileMetadata($path) !== null;
        } catch (OssException $exception) {
            throw UnableToCheckExistence::forLocation($path, $exception);
        }
    }

    /**
     * 判断文件夹是否存在
     * @param string $path
     * @return bool
     */
    public function directoryExists(string $path): bool
    {
        try {
            return $this->fileExists($path);
        } catch (UnableToCheckExistence $exception) {
            throw UnableToCheckExistence::forLocation($path, $exception);
        }
    }

    /**
     * 写入一个文件
     * @param string $path
     * @param string $contents
     * @param Config $config
     * @throws FilesystemException
     */
    public function write(string $path, string $contents, Config $config): void
    {
        $prefixedPath = $this->prefixer->prefixPath($path);
        try {
            $this->client->putObject($this->getBucket(), $prefixedPath, $contents, $config->get('headers', []));
        } catch (OssException $exception) {
            throw UnableToWriteFile::atLocation($path, $exception->getMessage());
        }
        if ($visibility = $config->get('visibility')) {
            $this->setVisibility($path, $visibility);
        }
    }

    /**
     * 写入文件
     * @param string $path
     * @param $contents
     * @param Config $config
     * @throws FilesystemException
     */
    public function writeStream(string $path, $contents, Config $config): void
    {
        $this->write($path, \stream_get_contents($contents), $config);
    }

    /**
     * 读取文件
     * @param string $path
     * @return string
     */
    public function read(string $path): string
    {
        $prefixedPath = $this->prefixer->prefixPath($path);
        try {
            $contents = $this->client->getObject($this->getBucket(), $prefixedPath);
        } catch (OssException $exception) {
            throw UnableToReadFile::fromLocation($path, $exception->getErrorMessage());
        }
        return $contents;
    }

    /**
     * 读取流
     * @param string $path
     * @return false|resource
     */
    public function readStream(string $path)
    {
        if (!$data = $this->read($path)) {
            return false;
        }
        $stream = fopen('php://temp', 'w+b');
        fwrite($stream, $data);
        rewind($stream);
        return $stream;
    }

    /**
     * 删除对象
     * @param string $path
     */
    public function delete(string $path): void
    {
        $prefixedPath = $this->prefixer->prefixPath($path);
        try {
            $this->client->deleteObject($this->getBucket(), $prefixedPath);
        } catch (OssException $exception) {
            throw UnableToDeleteFile::atLocation($path, $exception->getErrorMessage(), $exception);
        }
    }

    /**
     * 删除目录
     * @param string $path
     * @throws OssException
     */
    public function deleteDirectory(string $path): void
    {
        $dirname = $this->prefixer->prefixPath($path);
        $response = $this->listObjects($dirname, true);
        if (empty($response['Contents'])) {
            return;
        }
        $objects = array_map(function ($item) {
            return ['Key' => $item['Key']];
        }, $response['Contents']);

        try {
            $this->client->deleteObjects($this->getBucket(), $objects);
        } catch (OssException $exception) {
            throw UnableToDeleteDirectory::atLocation($path, $exception->getErrorMessage());
        }
    }

    /**
     * 创建文件夹
     * @param string $path
     * @param Config $config
     */
    public function createDirectory(string $path, Config $config): void
    {
        $dirname = $this->prefixer->prefixPath($path);
        try {
            $this->client->createObjectDir($this->getBucket(), $dirname);
        } catch (OssException $exception) {
            UnableToCreateDirectory::atLocation($path, $exception->getErrorMessage());
        }
    }

    /**
     * 设置访问策略
     * @param string $path
     * @param string $visibility
     */
    public function setVisibility(string $path, string $visibility): void
    {
        try {
            $this->client->putObjectAcl($this->getBucket(), $this->prefixer->prefixPath($path), $this->visibility->visibilityToAcl($visibility));
        } catch (OssException $exception) {
            throw UnableToSetVisibility::atLocation($path, $exception->getErrorMessage(), $exception);
        }
    }

    /**
     * 获取访问策略
     * @param string $path
     * @return FileAttributes
     */
    public function visibility(string $path): FileAttributes
    {
        try {
            $result = $this->client->getObjectAcl($this->getBucket(), $this->prefixer->prefixPath($path));
        } catch (OssException $exception) {
            throw UnableToRetrieveMetadata::visibility($path, $exception->getErrorMessage(), $exception);
        }
        $visibility = $this->visibility->aclToVisibility((array)$result['Grants']);
        return new FileAttributes($path, null, $visibility);
    }

    /**
     * 获取内容类型
     * @param string $path
     * @return FileAttributes
     */
    public function mimeType(string $path): FileAttributes
    {
        $attributes = $this->fetchFileMetadata($path, FileAttributes::ATTRIBUTE_MIME_TYPE);
        if ($attributes->mimeType() === null) {
            throw UnableToRetrieveMetadata::mimeType($path);
        }
        return $attributes;
    }

    /**
     * 获取最后更改
     * @param string $path
     * @return FileAttributes
     */
    public function lastModified(string $path): FileAttributes
    {
        $attributes = $this->fetchFileMetadata($path, FileAttributes::ATTRIBUTE_LAST_MODIFIED);
        if ($attributes->lastModified() === null) {
            throw UnableToRetrieveMetadata::lastModified($path);
        }
        return $attributes;
    }

    /**
     * 获取文件大小
     * @param string $path
     * @return FileAttributes
     */
    public function fileSize(string $path): FileAttributes
    {
        $attributes = $this->fetchFileMetadata($path, FileAttributes::ATTRIBUTE_FILE_SIZE);
        if ($attributes->fileSize() === null) {
            throw UnableToRetrieveMetadata::fileSize($path);
        }
        return $attributes;
    }

    /**
     * 获取对象访问 Url
     * @param string $path
     * @return string
     * @throws OssException
     */
    public function getUrl(string $path): string
    {
        $location = $this->prefixer->prefixPath($path);
        if (isset($this->config['url']) && !empty($this->config['url'])) {
            return $this->config['url'] . '/' . ltrim($location, '/');
        } else {
            $visibility = $this->visibility($path);
            if ($visibility && $visibility['visibility'] == 'private') {
                return $this->getTemporaryUrl($path, Carbon::now()->addMinutes(5), []);
            }
            $scheme = $this->config['ssl'] ? 'https://' : 'http://';
            return $scheme . $this->getBucket() . '.' . $this->config['endpoint'] . '/' . ltrim($location, '/');
        }
    }

    /**
     * 获取文件临时访问 Url
     * @param string $path
     * @param \DateTimeInterface $expiration
     * @param array $options
     * @return string
     * @throws OssException
     */
    public function getTemporaryUrl(string $path, \DateTimeInterface $expiration, array $options = []): string
    {
        $location = $this->prefixer->prefixPath($path);
        $timeout = $expiration->getTimestamp() - time();
        return $this->client->signUrl($this->getBucket(), $location, $timeout, OssClient::OSS_HTTP_GET, $options);
    }

    /**
     * 列出对象
     * @param string $path
     * @param bool $deep
     * @return iterable
     * @throws OssException
     */
    public function listContents(string $path, bool $deep): iterable
    {
        $prefixedPath = $this->prefixer->prefixPath($path);
        $response = $this->listObjects($prefixedPath, $deep);
        // 处理目录
        foreach ($response['CommonPrefixes'] ?? [] as $prefix) {
            yield new DirectoryAttributes($prefix['Prefix']);
        }
        //处理文件
        foreach ($response['Contents'] ?? [] as $content) {
            yield new FileAttributes(
                $content['Key'],
                \intval($content['Size']),
                null,
                \strtotime($content['LastModified'])
            );
        }
    }

    /**
     * 移动对象到新位置
     * @param string $source
     * @param string $destination
     * @param Config $config
     */
    public function move(string $source, string $destination, Config $config): void
    {
        $this->copy($source, $destination, $config);
        $this->delete($this->prefixer->prefixPath($source));
    }

    /**
     * 复制对象到新位置
     * @param string $source
     * @param string $destination
     * @param Config $config
     */
    public function copy(string $source, string $destination, Config $config): void
    {
        $prefixedSource = $this->prefixer->prefixPath($source);
        $location = $this->getSourcePath($prefixedSource);
        $prefixedDestination = $this->prefixer->prefixPath($destination);
        try {
            $this->client->copyObject($this->getBucket(), $prefixedSource, $this->getBucket(), $prefixedDestination);
        } catch (OssException $exception) {
            throw UnableToCopyFile::fromLocationTo($source, $destination);
        }
    }

    /**
     * 列出对象
     * @param string $directory
     * @param bool $recursive
     * @return array
     * @throws OssException
     */
    protected function listObjects(string $directory = '', bool $recursive = false)
    {
        $objectListInfo = $this->client->listObjects($this->getBucket(), [
            'prefix' => ('' === (string)$directory) ? '' : ($directory . '/'),
            'delimiter' => $recursive ? '' : '/',
            'max-keys' => 1000,
        ]);
        $result = [];
        // 处理目录
        foreach ($objectListInfo->getPrefixList() ?? [] as $prefix) {
            $result['CommonPrefixes'][] = ['Prefix' => $prefix->getPrefix()];
        }
        //处理文件
        foreach ($objectListInfo->getObjectList() ?? [] as $content) {
            $result['Contents'][] = [
                'Key' => $content->getKey(),
                'LastModified' => $content->getLastModified(),
                'Size' => $content->getSize(),
            ];
        }
        return $result;
    }

    /**
     * 获取文件 MetaData
     * @param string $path
     * @param string $type
     * @return FileAttributes|null
     */
    private function fetchFileMetadata(string $path, string $type): ?FileAttributes
    {
        try {
            $meta = $this->client->getObjectMeta($this->getBucket(), $this->prefixer->prefixPath($path));
        } catch (OssException $exception) {
            throw UnableToRetrieveMetadata::create($path, $type, $exception->getErrorMessage(), $exception);
        }
        $attributes = $this->mapObjectMetadata($meta, $path);
        if (!$attributes instanceof FileAttributes) {
            throw UnableToRetrieveMetadata::create($path, $type, '');
        }
        return $attributes;
    }

    /**
     * 映射Meta
     * @param array $metadata
     * @param string|null $path
     * @return StorageAttributes
     */
    private function mapObjectMetadata(array $metadata, string $path = null): StorageAttributes
    {
        if ($path === null) {
            $path = $this->prefixer->stripPrefix($metadata['Key'] ?? $metadata['Prefix']);
        }
        if (str_ends_with($path, '/')) {
            return new DirectoryAttributes(rtrim($path, '/'));
        }
        $mimetype = $metadata['content-type'] ?? null;
        $fileSize = $metadata['content-length'] ?? null;
        $fileSize = $fileSize === null ? null : (int)$fileSize;
        $dateTime = $metadata['last-modified'] ?? null;
        $lastModified = $dateTime ? strtotime($dateTime) : null;
        return new FileAttributes($path, $fileSize, null, $lastModified, $mimetype, $this->extractExtraMetadata($metadata));
    }

    /**
     * 导出扩展 Meta Data
     * @param array $metadata
     * @return array
     */
    private function extractExtraMetadata(array $metadata): array
    {
        $extracted = [];
        foreach (static::EXTRA_METADATA_FIELDS as $field) {
            if (isset($metadata[$field]) && $metadata[$field] !== '') {
                $extracted[$field] = $metadata[$field];
            }
        }
        return $extracted;
    }

    /**
     * 获取 OSS 客户端
     * @return OssClient
     */
    public function getClient(): OssClient
    {
        return $this->client;
    }

    /**
     * 设置客户端
     * @param OssClient $objectClient
     * @return OSSAdapter
     */
    public function setClient(OssClient $objectClient): OSSAdapter
    {
        $this->client = $objectClient;
        return $this;
    }

    /**
     * Get the Aliyun Oss Client bucket.
     *
     * @return string
     */
    public function getBucket(): string
    {
        return $this->config['bucket'];
    }
}
