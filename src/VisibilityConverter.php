<?php

declare(strict_types=1);
/**
 * This is NOT a freeware, use is subject to license terms
 */

namespace Larva\Flysystem\Oss;

interface  VisibilityConverter
{
    public function visibilityToAcl(string $visibility): string;

    public function aclToVisibility(string $acl): string;

    public function defaultForDirectories(): string;
}