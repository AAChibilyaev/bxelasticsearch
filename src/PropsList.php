<?php
/*
 * Copyright (c) 2021, AAChibilyaev
 *
 * Chibilyaev Alexandr <info@aachibilyaev.com>
 *
 * AAChibilyaev LTD <https://aachibilyaev.com/>
 */

namespace aachibilyaev\bxelasticsearch;

use CIBlockElement;
use CIBlockProperty;
use CIBlockPropertyEnum;

class PropsList extends ElkClient
{
	public function __construct(){parent::__construct();}
	public function reCreateIndex(): void
	{
		$this->removeIndex();
		$this->createIndex();
	}
	public function removeIndex(): void
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		if($elastic->indices()->exists(['index' => $config['INDEX_PROPS_LIST']]))
			$elastic->indices()->delete(['index' => $config['INDEX_PROPS_LIST']]);
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
				'index' => $config['INDEX_PROPS_LIST'], 
				'body' => 
				[
					'settings' => array
					(
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
						]
					),
				]
			]
		);
		$result = [];
		$result['PROPERTY_CODE']['type'] = 'keyword';
		$result['ID']['type'] = 'integer';
		$result['PROPERTY_ID']['type'] = 'keyword';
		$result['VALUE']['type'] = 'keyword';
		$result['DEF']['type'] = 'keyword';
		$result['SORT']['type'] = 'integer';
		$result['XML_ID']['type'] = 'keyword';
		$result['TMP_ID']['type'] = 'keyword';
		$result['EXTERNAL_ID']['type'] = 'keyword';
		$result['PROPERTY_NAME']['type'] = 'keyword';
		$result['PROPERTY_SORT']['type'] = 'integer';
		$elastic->indices()->putMapping(
			[
				'index' => $config['INDEX_PROPS_LIST'], 
				'body' => 
				[
					'properties' => $result
				]
			]
		);
	}
	public function importData(): void
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$props = CIBlockPropertyEnum::GetList(
			array('ID' => 'DESC', 'SORT' => 'ASC'), 
			array('IBLOCK_ID' => 2,)
		);
		while($prop = $props->GetNext())
		{
			foreach($prop as $key => $field)
				if($key[0] == '~')
					unset($prop[$key]);
			$bulk = ['body' => []];
			$bulk['body'][] = 
			[
				'create' => 
				[
					'_index' => $config['INDEX_PROPS_LIST'], 
					'_id' => $prop['ID']
				]
			];
			$bulk['body'][] = $prop;
			$elastic->bulk($bulk);
		}
	}
	public function getAllPropsList(): array
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();

		$params = [
			'index' => $config['INDEX_PROPS_LIST'], 
			'size' => 199999, 
			'body' => []
		];
		$response = $elastic->search($params);
		if($response['hits']['total']['value'] > 0)
			return $response['hits']['hits'];
		else
			return [];
	}
	public function getPropsListByXmlId($xmlId): array
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$params = 
		[
			'index' => $config['INDEX_PROPS_LIST'], 
			'body' => 
				[
					'query' => 
						[
							'bool' => 
								[
									'must' => 
										[
											[
												'match' => ['XML_ID' => $xmlId]
											]
										]
								],
						]
				], 
			'_source' => ['PROPS']
		];
		$response = $elastic->search($params);
		if($response['hits']['total']['value'] > 0)
			return $response['hits']['hits'];
		else
			return array();
	}
	public function getPropsListByPropertyName($propertyName): array
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$params = 
		[
			'index' => $config['INDEX_PROPS_LIST'], 
			'body' => 
				[
					'query' => 
						[
							'bool' => 
								[
									'must' => 
										[
											[
												'match' => ['PROPERTY_NAME' => $propertyName]
											]
										]
								],
						]
				], 
				'_source' => ['PROPS']
		];
		$response = $elastic->search($params);
		if($response['hits']['total']['value'] > 0)
			return $response['hits']['hits'];
		else
			return array();

	}
	public function getPropsListByPropertyCode($propertyCode): array
	{

		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$params = 
		[
			'index' => $config['INDEX_PROPS_LIST'], 
			'body' => 
			[
				'query' => 
					[
						'bool' => 
							[
								'must' => 
								[
									[
										'match' => ['PROPERTY_CODE' => $propertyCode]
									]
								]
							],
					]
			], 
			'_source' => ['PROPS']
		];
		$response = $elastic->search($params);
		if($response['hits']['total']['value'] > 0)
			return $response['hits']['hits'];
		else
			return array();
	}
	public function getPropsListByPropertyCodeAndXmlId($propertyCode, $xmlId): array
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$params = 
		[
			'index' => $config['INDEX_PROPS_LIST'], 
			'body' => 
				[
					'query' => 
						[
							'bool' => 
								[
									'must' => 
										[
											[
												'match' => ['PROPERTY_CODE' => $propertyCode]
											], 
											[
												'match' => ['XML_ID' => $xmlId]
											],
										]
								],
						]
				],
				'_source' => ['PROPS']
		];
		$response = $elastic->search($params);
		if($response['hits']['total']['value'] > 0)
			return $response['hits']['hits'];
		else
			return array();
	}
	public function getPropsListByPropertyCodeAndValue($propertyCode, $value): array
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$params = 
			[
				'index' => $config['INDEX_PROPS_LIST'], 
				'body' => 
					[
						'query' => 
							[
								'bool' => 
									[
										'must' => 
											[
												[
													'match' => ['PROPERTY_CODE' => $propertyCode]
												], 
												[
													'match' => ['VALUE' => $value]
												],
											]
									],
							]
					],
			];
		$response = $elastic->search($params);
		if($response['hits']['total']['value'] > 0)
			return $response['hits']['hits'][0]['_source'];
		else
			return array();
	}

}