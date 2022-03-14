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
use League\Flysystem\InvalidVisibilityProvided;
use League\Flysystem\PathPrefixer;
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
use League\Flysystem\UnixVisibility\PortableVisibilityConverter;
use League\MimeTypeDetection\FinfoMimeTypeDetector;
use OSS\Core\OssException;
use OSS\OssClient;

/**
 * 阿里云适配器
 */
class OSSAdapter implements FilesystemAdapter
{
    /**
     * @var OssClient|null
     */
    protected ?OssClient $objectClient;

    /**
     * @var array
     */
    protected array $config = [];
    protected PathPrefixer $prefixer;
    protected VisibilityConverter $visibility;
    protected MimeTypeDetector $mimeTypeDetector;

    /**
     * Adapter constructor.
     *
     * @param array $config
     * @param VisibilityConverter|null $visibility
     * @param MimeTypeDetector|null $mimeTypeDetector
     */
    public function __construct(array $config, VisibilityConverter $visibility = null, MimeTypeDetector $mimeTypeDetector = null)
    {
        $this->config = $config;
        $this->prefixer = new PathPrefixer($config['prefix'] ?? '', DIRECTORY_SEPARATOR);
        $this->visibility = $visibility ?: new PortableVisibilityConverter();
        $this->mimeTypeDetector = $mimeTypeDetector ?: new FinfoMimeTypeDetector();
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
            return $this->getMetadata($path) !== null;
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
            $this->getObjectClient()->putObject($this->getBucket(), $prefixedPath, $contents, $config->get('headers', []));
        } catch (OssException $e) {
            throw UnableToWriteFile::atLocation($path, $e->getMessage());
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
            $contents = $this->getObjectClient()->getObject($this->getBucket(), $prefixedPath);
        } catch (OssException $e) {
            throw UnableToReadFile::fromLocation($path, $e->getMessage());
        }
        return $contents;
    }

    public function readStream(string $path)
    {
        // TODO: Implement readStream() method.
    }

    /**
     * 删除文件夹
     * @param string $path
     */
    public function delete(string $path): void
    {
        $prefixedPath = $this->prefixer->prefixPath($path);
        try {
            $this->getObjectClient()->deleteObject($this->getBucket(), $prefixedPath);
        } catch (OssException $exception) {
            throw UnableToDeleteFile::atLocation($path, $exception->getErrorMessage(), $exception);
        } catch (\Throwable $exception) {
            throw UnableToDeleteFile::atLocation($path, '', $exception);
        }
    }

    public function deleteDirectory(string $path): void
    {
        // TODO: Implement deleteDirectory() method.
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
            $this->getObjectClient()->putObject($this->getBucket(), $dirname . '/', '');
        } catch (OssException $e) {
            UnableToCreateDirectory::atLocation($path, $e->getMessage());
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
            $this->getObjectClient()->putObjectACL($this->getBucket(), $this->prefixer->prefixPath($path), $visibility);
        } catch (OssException $e) {
            UnableToSetVisibility::atLocation($path, $e->getMessage());
        }
    }

    public function visibility(string $path): FileAttributes
    {
        try {
            $acl = $this->getObjectClient()->getObjectAcl($this->getBucket(), $this->prefixer->prefixPath($path));
        } catch (OssException $exception) {
            throw UnableToRetrieveMetadata::visibility($path, $exception->getErrorMessage(), $exception);
        } catch (\Throwable $exception) {
            throw UnableToRetrieveMetadata::visibility($path, '', $exception);
        }
        $visibility = $this->visibility->aclToVisibility($acl);
        return new FileAttributes($path, null, $visibility);
    }

    /**
     * 获取内容类型
     * @param string $path
     * @return FileAttributes
     * @throws OssException
     */
    public function mimeType(string $path): FileAttributes
    {
        $meta = $this->getMetadata($path);
        if ($meta->mimeType() === null) {
            throw UnableToRetrieveMetadata::mimeType($path);
        }
        return $meta;
    }

    /**
     * 获取最后更改
     * @param string $path
     * @return FileAttributes
     * @throws OssException
     */
    public function lastModified(string $path): FileAttributes
    {
        $meta = $this->getMetadata($path);
        if ($meta->lastModified() === null) {
            throw UnableToRetrieveMetadata::lastModified($path);
        }

        return $meta;
    }

    /**
     * 获取文件大小
     * @param string $path
     * @return FileAttributes
     * @throws OssException
     */
    public function fileSize(string $path): FileAttributes
    {
        $meta = $this->getMetadata($path);
        if ($meta->fileSize() === null) {
            throw UnableToRetrieveMetadata::fileSize($path);
        }

        return $meta;
    }

    /**
     * 列出对象
     * @param string $path
     * @param bool $deep
     * @return iterable
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

    public function move(string $source, string $destination, Config $config): void
    {
        // TODO: Implement move() method.
    }

    public function copy(string $source, string $destination, Config $config): void
    {
        // TODO: Implement copy() method.
    }

    protected function listObjects(string $directory = '', bool $recursive = false)
    {
        $result = $this->getObjectClient()->listObjects($this->getBucket(),
            [
                'prefix' => ('' === (string)$directory) ? '' : ($directory . '/'),
                'delimiter' => $recursive ? '' : '/',
            ]
        );

        foreach (['CommonPrefixes', 'Contents'] as $key) {
            $result[$key] = $result[$key] ?? [];
            // 确保是二维数组
            if (($index = \key($result[$key])) !== 0) {
                $result[$key] = \is_null($index) ? [] : [$result[$key]];
            }
        }

        return $result;
    }

    /**
     * 获取文件 MetaData
     * @param string $path
     * @return FileAttributes|null
     * @throws OssException
     */
    protected function getMetadata(string $path): ?FileAttributes
    {
        $prefixedPath = $this->prefixer->prefixPath($path);
        $meta = $this->getObjectClient()->getObjectMeta($this->getBucket(), $prefixedPath);
        if (empty($meta)) {
            return null;
        }
        return new FileAttributes($path,
            isset($meta['content-length'][0]) ? \intval($meta['content-length'][0]) : null,
            null,
            isset($meta['last-modified'][0]) ? \strtotime($meta['last-modified'][0]) : null,
            $meta['content-type'][0] ?? null,
        );
    }

    /**
     * 获取 OSS 客户端
     * @return OssClient
     * @throws OssException
     */
    public function getObjectClient(): OssClient
    {
        if (!$this->objectClient) {
            $this->objectClient = new OssClient(
                $this->config['access_id'],
                $this->config['access_key'],
                $this->config['endpoint'],
                false,
                $this->config['security_token'] ?? null,
                $this->config['proxy'] ?? null
            );
        }
        return $this->objectClient;
    }

    /**
     * 设置客户端
     * @param OssClient $objectClient
     * @return OSSAdapter
     */
    public function setObjectClient(OssClient $objectClient): OSSAdapter
    {
        $this->objectClient = $objectClient;
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
