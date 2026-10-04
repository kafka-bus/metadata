<?php

declare(strict_types=1);

namespace KafkaBus\Metadata\Partitions;

enum Offset
{
    case Early;
    case Latest;
}
