<?php


namespace Ecotone\EventSourcing;


use Prooph\Common\Messaging\Message;
use Prooph\Common\Messaging\MessageConverter;

class ProophEventConverter implements MessageConverter
{
    public function convertToArray(Message $domainMessage): array
    {
//        return
    }
}