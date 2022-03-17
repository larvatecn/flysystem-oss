<?php
/**
 * This is NOT a freeware, use is subject to license terms
 * @copyright Copyright (c) 2010-2099 Jinan Larva Information Technology Co., Ltd.
 * @link http://www.larva.com.cn/
 */

namespace Larva\Flysystem\Oss;

use League\Flysystem\Config;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\FileAttributes;
use League\Flysystem\FilesystemAdapter;
use League\Flysystem\FilesystemException;
use League\Flysystem\FilesystemOperationFailed;
use League\Flysystem\PathPrefixer;
use League\Flysystem\StorageAttributes;
use League\Flysystem\UnableToCheckExistence;
use League\Flysystem\UnableToCopyFile;
use League\Flysystem\UnableToCreateDirectory;
use League\Flysystem\UnableToDeleteDirectory;
use League\Flysystem\UnableToDeleteFile;
use League\Flysystem\UnableToMoveFile;
use League\Flysystem\UnableToReadFile;
use League\Flysystem\UnableToRetrieveMetadata;
use League\Flysystem\UnableToSetVisibility;
use League\Flysystem\UnableToWriteFile;
use League\Flysystem\Visibility;
use League\MimeTypeDetection\FinfoMimeTypeDetector;
use League\MimeTypeDetection\MimeTypeDetector;
use OSS\Core\OssException;
use OSS\OssClient;
use Throwable;

/**
 * 阿里云适配器
 */
class AliyunOSSAdapter implements FilesystemAdapter
{
    /**
     * @var string[]
     */
    public const AVAILABLE_OPTIONS = [
        'Cache-Control', 'Content-Disposition', 'Content-Encoding', 'Content-MD5', 'Content-Length', 'ETag', 'Expires',
        'x-oss-forbid-overwrite', 'x-oss-server-side-encryption', 'x-oss-server-side-data-encryption', 'x-oss-server-side-encryption-key-id',
        'x-oss-object-acl', 'x-oss-storage-class', 'x-oss-tagging'
    ];

    /**
     * 扩展 MetaData 字段
     * @var string[]
     */
    private const EXTRA_METADATA_FIELDS = [
        'x-oss-object-type',
        'x-oss-storage-class',
        'x-oss-hash-crc64ecma',
        'x-oss-version-id',
        'etag',
        'content-md5',
    ];

    /**
     * @var OssClient
     */
    private OssClient $client;

    /**
     * @var PathPrefixer
     */
    private PathPrefixer $prefixer;

    /**
     * @var string
     */
    private string $bucket;

    /**
     * @var VisibilityConverter
     */
    private VisibilityConverter $visibility;

    /**
     * @var MimeTypeDetector
     */
    private MimeTypeDetector $mimeTypeDetector;

    /**
     * @var array
     */
    private array $options;

    /**
     * Adapter constructor.
     *
     * @param OssClient $client
     * @param string $bucket
     * @param string $prefix
     * @param VisibilityConverter|null $visibility
     * @param MimeTypeDetector|null $mimeTypeDetector
     * @param array $options
     */
    public function __construct(OssClient $client, string $bucket, string $prefix = '', VisibilityConverter $visibility = null, MimeTypeDetector $mimeTypeDetector = null, array $options = [])
    {
        $this->client = $client;
        $this->prefixer = new PathPrefixer($prefix);
        $this->bucket = $bucket;
        $this->visibility = $visibility ?: new PortableVisibilityConverter();
        $this->mimeTypeDetector = $mimeTypeDetector ?: new FinfoMimeTypeDetector();
        $this->options = $options;
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
            return $this->client->doesObjectExist($this->bucket, $this->prefixer->prefixPath($path), $this->options);
        } catch (Throwable $exception) {
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
            $prefix = $this->prefixer->prefixDirectoryPath($path);
            $options = ['prefix' => $prefix, 'delimiter' => '/'];
            $objectListInfo = $this->client->listObjects($this->bucket, $options);
            return count($objectListInfo->getObjectList()) > 0;
        } catch (OssException $exception) {
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
        $this->upload($path, $contents, $config);
    }

    /**
     * @param string $path
     * @param string|resource $body
     * @param Config $config
     */
    private function upload(string $path, $body, Config $config): void
    {
        $object = $this->prefixer->prefixPath($path);
        $options = $this->createOptionsFromConfig($config);
        $options['x-oss-object-acl'] = $this->determineAcl($config);
        $shouldDetermineMimetype = $body !== '' && !array_key_exists('ContentType', $options);
        if ($shouldDetermineMimetype && $mimeType = $this->mimeTypeDetector->detectMimeType($object, $body)) {
            $options['ContentType'] = $mimeType;
        }
        try {
            $this->client->putObject($this->bucket, $object, $body, $options);
        } catch (Throwable $exception) {
            throw UnableToWriteFile::atLocation($path, '', $exception);
        }
    }

    /**
     * 转换ACL
     * @param Config $config
     * @return string
     */
    private function determineAcl(Config $config): string
    {
        $visibility = (string)$config->get(Config::OPTION_VISIBILITY, Visibility::PRIVATE);
        return $this->visibility->visibilityToAcl($visibility);
    }

    /**
     * 从配置创建参数
     * @param Config $config
     * @return array
     */
    private function createOptionsFromConfig(Config $config): array
    {
        $options = [];
        foreach (static::AVAILABLE_OPTIONS as $option) {
            $value = $config->get($option, '__NOT_SET__');

            if ($value !== '__NOT_SET__') {
                $options[$option] = $value;
            }
        }
        return $options + $this->options;
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
        $this->upload($path, \stream_get_contents($contents), $config);
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
            return $this->client->getObject($this->bucket, $prefixedPath);
        } catch (Throwable $exception) {
            throw UnableToReadFile::fromLocation($path, $exception->getMessage());
        }
    }

    /**
     * 读取流
     * @param string $path
     * @return resource
     */
    public function readStream(string $path)
    {
        try {
            $data = $this->read($path);
            $stream = fopen('php://temp', 'w+b');
            fwrite($stream, $data);
            rewind($stream);
            return $stream;
        } catch (Throwable $exception) {
            throw UnableToReadFile::fromLocation($path, $exception->getMessage());
        }
    }

    /**
     * 删除对象
     * @param string $path
     */
    public function delete(string $path): void
    {
        $prefixedPath = $this->prefixer->prefixPath($path);
        try {
            $this->client->deleteObject($this->bucket, $prefixedPath);
        } catch (Throwable $exception) {
            throw UnableToDeleteFile::atLocation($path, '', $exception);
        }
    }

    /**
     * 删除目录
     * @param string $path
     */
    public function deleteDirectory(string $path): void
    {
        try {
            $prefix = $this->prefixer->prefixPath($path);
            $objectListInfo = $this->listObjects($prefix, true);
            if (empty($objectListInfo['Contents'])) {
                return;
            }
            $objects = array_map(function ($item) {
                return $item['Key'];
            }, $objectListInfo['Contents']);
            $this->client->deleteObjects($this->bucket, $objects);
        } catch (Throwable $exception) {
            throw UnableToDeleteDirectory::atLocation($path, '', $exception);
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
            $this->client->createObjectDir($this->bucket, $dirname);
        } catch (Throwable $exception) {
            UnableToCreateDirectory::atLocation($path, $exception->getMessage());
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
            $this->client->putObjectAcl($this->bucket, $this->prefixer->prefixPath($path), $this->visibility->visibilityToAcl($visibility));
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
            $acl = $this->client->getObjectAcl($this->bucket, $this->prefixer->prefixPath($path));
        } catch (OssException $exception) {
            throw UnableToRetrieveMetadata::visibility($path, $exception->getErrorMessage(), $exception);
        }
        $visibility = $this->visibility->aclToVisibility($acl);
        return new FileAttributes($path, null, $visibility);
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
            $meta = $this->client->getObjectMeta($this->bucket, $this->prefixer->prefixPath($path));
        } catch (Throwable $exception) {
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
            yield new FileAttributes($content['Key'], intval($content['Size']), null, strtotime($content['LastModified']));
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
        try {
            $this->copy($source, $destination, $config);
            $this->delete($source);
        } catch (FilesystemOperationFailed $exception) {
            throw UnableToMoveFile::fromLocationTo($source, $destination, $exception);
        }
    }

    /**
     * 复制对象到新位置
     * @param string $source
     * @param string $destination
     * @param Config $config
     */
    public function copy(string $source, string $destination, Config $config): void
    {
        try {
            $this->client->copyObject($this->bucket, $this->prefixer->prefixPath($source), $this->bucket, $this->prefixer->prefixPath($destination));
        } catch (Throwable $exception) {
            throw UnableToCopyFile::fromLocationTo($source, $destination, $exception);
        }
    }

    /**
     * 列出对象
     * @param string $directory
     * @param bool $recursive
     * @return array
     * @throws OssException
     */
    private function listObjects(string $directory = '', bool $recursive = false): array
    {
        $objectListInfo = $this->client->listObjects($this->bucket, [
            'prefix' => ('' === $directory) ? '' : ($directory . '/'),
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
}
