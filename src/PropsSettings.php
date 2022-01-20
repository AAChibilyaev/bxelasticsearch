<?php
/*
 * Copyright (c) 2021, AAChibilyaev
 *
 * Chibilyaev Alexandr <info@aachibilyaev.com>
 *
 * AAChibilyaev LTD <https://aachibilyaev.com/>
 */

namespace aachibilyaev\bxelasticsearch;

use CIBlockProperty;

class PropsSettings extends ElkClient
{
	public function __construct()
	{
		parent::__construct();
	}

	public function reCreateIndex(): void
	{
		$this->removeIndex();
		$this->createIndex();
	}

	public function removeIndex(): void
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		if($elastic->indices()->exists(['index' => $config['INDEX_PROPS_SETTINGS']]))
			$elastic->indices()->delete(['index' => $config['INDEX_PROPS_SETTINGS']]);
	}

	public function createIndex(): void
	{
		$this->setElasticMapping();
		$this->importData();
	}

	public function setElasticMapping(): void
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$elastic->indices()->create(
			[
				'index' => $config['INDEX_PROPS_SETTINGS'], 
				'body' => 
					[
						'settings' => array(
							'number_of_shards' => 1, 
							'number_of_replicas' => 0, 
							'index.mapping.total_fields.limit' => 600000, 
							'index.max_result_window' => 500000, 
							'analysis' => 
								[
									'analyzer' => 
										[
											'my_analyzer' => 
												[
													'tokenizer' => 'my_tokenizer'
												]
										], 
									'tokenizer' => 
									[
										'my_tokenizer' => 
										[
											'type' => 'ngram', 
											'min_gram' => 3, 
											'max_gram' => 3, 
											'token_chars' => 
											[
												'letter', 'digit'
											]
										]
									]
								]),
						]
				]);
		$result = [];
		$result['ID']['type'] = 'integer';
		$result['NAME']['type'] = 'keyword';
		$result['SORT']['type'] = 'integer';
		$result['CODE']['type'] = 'keyword';
		$result['PROPERTY_TYPE']['type'] = 'keyword';
		$result['LIST_TYPE']['type'] = 'keyword';
		$result['MULTIPLE']['type'] = 'keyword';
		$result['XML_ID']['type'] = 'keyword';
		$result['TMP_ID']['type'] = 'keyword';
		$result['LINK_IBLOCK_ID']['type'] = 'integer';
		$result['WITH_DESCRIPTION']['type'] = 'keyword';
		$result['SEARCHABLE']['type'] = 'keyword';
		$result['FILTRABLE']['type'] = 'keyword';
		$result['IS_REQUIRED']['type'] = 'keyword';
		$result['HINT']['type'] = 'keyword';
		$elastic->indices()->putMapping(['index' => $config['INDEX_PROPS_SETTINGS'], 'body' => ['properties' => $result]]);
	}

	public function importData(): void
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$properties = CIBlockProperty::GetList
		(
			array(), 
			array('ACTIVE' => 'Y', 'IBLOCK_ID' => $config['IBLOCK_ID_CATALOG'])
		);
		while($prop_fields = $properties->GetNext())
		{
			$prop['ID'] = $prop_fields['ID'];
			$prop['NAME'] = $prop_fields['NAME'];
			$prop['SORT'] = $prop_fields['SORT'];
			$prop['CODE'] = $prop_fields['CODE'];
			$prop['PROPERTY_TYPE'] = $prop_fields['PROPERTY_TYPE'];
			$prop['LIST_TYPE'] = $prop_fields['LIST_TYPE'];
			$prop['MULTIPLE'] = $prop_fields['MULTIPLE'];
			$prop['XML_ID'] = $prop_fields['XML_ID'];
			$prop['TMP_ID'] = $prop_fields['TMP_ID'];
			$prop['LINK_IBLOCK_ID'] = $prop_fields['LINK_IBLOCK_ID'];
			$prop['WITH_DESCRIPTION'] = $prop_fields['WITH_DESCRIPTION'];
			$prop['SEARCHABLE'] = $prop_fields['SEARCHABLE'];
			$prop['FILTRABLE'] = $prop_fields['FILTRABLE'];
			$prop['IS_REQUIRED'] = $prop_fields['IS_REQUIRED'];
			$prop['HINT'] = $prop_fields['HINT'];
			$bulk = ['body' => []];
			$bulk['body'][] = ['create' => ['_index' => $config['INDEX_PROPS_SETTINGS'], '_id' => $prop['ID']]];
			$bulk['body'][] = $prop;
			$elastic->bulk($bulk);
		}
	}

	public function getPropsSettings(): array
	{
		$result = [];
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$params = ['index' => $config['INDEX_PROPS_SETTINGS'], 'size' => 11222];
		$response = $elastic->search($params);
		$r = [];
		if($response['hits']['total']['value'] > 0)
		{
			$result = $response['hits']['hits'];
			foreach($result as $props)
				$r[$props['_source']['CODE']] = $props['_source'];
			$result = $r;
		}
		else
		{
			$result = false;
		}
		return $result;
	}

	public function getPropsSettingsByCode($propertyCode): array
	{
		$result = [];
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$params = 
			[
				'index' => $config['INDEX_PROPS_SETTINGS'], 
				'body' => 
					[
						'query' => 
							[
								'bool' => 
									[
										'must' => 
											[
												[
													'match' => 
														[
															'CODE' => $propertyCode
														]
												]
											]
									]
							]
					]
			];
		$response = $elastic->search($params);
		$r = [];
		if($response['hits']['total']['value'] > 0)
		{
			$result = $response['hits']['hits'];
			foreach($result as $props)
				$r[$props['_source']['CODE']] = $props['_source'];
			$result = $r;
		}
		else
		{
			$result = false;
		}
		return $result;
	}
}