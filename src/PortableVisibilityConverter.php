<?php

namespace Larva\Flysystem\Oss;

use League\Flysystem\Visibility;

class PortableVisibilityConverter implements VisibilityConverter
{
    private const PUBLIC_ACL = 'public-read';
    private const PRIVATE_ACL = 'private';

    /**
     * @var string
     */
    private string $defaultForDirectories;

    public function __construct(string $defaultForDirectories = Visibility::PUBLIC)
    {
        $this->defaultForDirectories = $defaultForDirectories;
    }

    public function visibilityToAcl(string $visibility): string
    {
        if ($visibility === Visibility::PUBLIC) {
            return self::PUBLIC_ACL;
        }

        return self::PRIVATE_ACL;
    }

    public function aclToVisibility(string $acl): string
    {
        if ($acl == 'default') {
            $this->defaultForDirectories();
        } else if ($acl === self::PUBLIC_ACL) {
            return Visibility::PUBLIC;
        }
        return Visibility::PRIVATE;
    }

    public function defaultForDirectories(): string
    {
        return $this->defaultForDirectories;
    }
}