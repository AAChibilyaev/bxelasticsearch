<?php

namespace aachibilyaev\bxelasticsearch;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;

class ElkClient
{
	private $elastic;
	private $config;

	public function __construct($strictMode = true)
	{
		$config = new \aachibilyaev\bxelasticsearch\Config();
		$config = $config->getConfig();
		$elastic = ClientBuilder::create()->setHosts(
			[
				[
					'host' => $config['HOST'],
					'port' => $config['PORT'],
					'user' => $config['USER'],
					'pass' => $config['PASS']
				]
			]
		)->build();
		
		$this->elastic = $elastic;
		$this->config = $config;
	}

	public function client()
	{
		return $this->elastic;
	}

	public function getConfig(): array
	{
		return $this->config;
	}
}