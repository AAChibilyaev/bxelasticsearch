<?php
/*
 * Copyright (c) 2021, AAChibilyaev
 *
 * Chibilyaev Alexandr <info@aachibilyaev.com>
 *
 * AAChibilyaev LTD <https://aachibilyaev.com/>
 */


namespace aachibilyaev\bxelasticsearch;
class Statistic extends ElkClient
{
	public function __construct()
	{
		parent::__construct();
	}
	public function reCreateIndex() //Пересоздание индекса
	{
		$this->removeIndex();
		$this->createIndex();
	}
	public function removeIndex() //Удаление индекса
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$elastic->indices()->delete(['index' => $config['TERMOKIT_STATISTIC']]);
	}
	public function createIndex()
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$elastic->indices()->create(['index' => $config['TERMOKIT_STATISTIC'], 'body' => ['settings' => array('number_of_shards' => 1, 'number_of_replicas' => 0, 'index.mapping.total_fields.limit' => 600000, 'index.max_result_window' => 500000, 'analysis' => ['analyzer' => ['my_analyzer' => ['tokenizer' => 'my_tokenizer']], 'tokenizer' => ['my_tokenizer' => ['type' => 'ngram', 'min_gram' => 3, 'max_gram' => 3, 'token_chars' => ['letter', 'digit']]]]),]]);
		$mappingData = $this->elasticMapping();
		$elastic->indices()->putMapping(['index' => $config['TERMOKIT_STATISTIC'], 'body' => $mappingData]);
	}
	public function elasticMapping(): array //ELK типы для iblock=2 свойств
	{
		$result = [];
		$result['properties']['VYSOTA_MM']['type'] = 'float';
		$result['properties']['SHIRINA_MM']['type'] = 'float';
		$result['properties']['VES_KG']['type'] = 'float';
		return $result;
	}

}