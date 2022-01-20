<?php
namespace aachibilyaev\bxelasticsearch;
use CIBlockElement;
use CIBlockProperty;
use CIBlockPropertyEnum;
use CIBlockSection;

class Sections extends ElkClient
{
	public function __construct() 
	{ 
		parent::__construct(); 
	}
	public function reCreateIndex(): void 
	{ 
		$this->removeIndex(); $this->createIndex(); 
	}
	public function removeIndex(): void
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		if($elastic->indices()->exists(['index' => $config['INDEX_SECTIONS_NAME']]))
			$elastic->indices()->delete(['index' => $config['INDEX_SECTIONS_NAME']]);
	}
	public function createIndex(): void
	{
		$this->setElasticMapping();
		$this->importData();
		// sleep(100);
		// $this->setSectionFilterJson();
	}
	public function setElasticMapping(): void
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$elastic->indices()->create(
			[
				'index' => $config['INDEX_SECTIONS_NAME'], 
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
		$sort = ['SORT' => 'ASC'];
		$filter = ['IBLOCK_ID' => $config['IBLOCK_ID_CATALOG']];
		$select = ['*', 'UF_*'];
		$sections = CIBlockSection::GetList($sort, $filter, false, $select, array('nTopCount' => 500000));
		while($section = $sections->GetNext())
		{
			foreach($section as $code => $val)
			{
				if($code[0] == '~')
				{
					unset($section[$code]);
					continue;
				}
				$typeTmp = $this->elasticTypeByVal($val);
				if(is_array($val))
				{
					$result[$code]['type'] = $typeTmp;
				}
				else
				{
					if(!empty($val))
						$result[$code]['type'] = $typeTmp;
				}
			}
		}
		// $result['FILTER']['type'] = 'text';
		// $result['PROPS']['type'] = 'nested';
		$elastic->indices()->putMapping(
			[
				'index' => $config['INDEX_SECTIONS_NAME'], 
				'body' => ['properties' => $result]
			]
		);
	}
	public function elasticTypeByVal($propVal = ''): string
	{
		$propVal = trim($propVal);
		$propValExplode = explode(' ', $propVal);
		if(count($propValExplode) < 5)
		{
			if(is_bool($propVal))
				return 'boolean';
			else
			{
				if(is_numeric($propVal))
				{
					if(is_float($propVal)) 
						return 'float';
					if(is_int($propVal)) 
						return 'integer';
					if($propVal == 1) 
						return 'boolean';
					else
					{
						if($propVal == 0) 
							return 'boolean';
						else 
							return 'integer';
					}
				}
				else
				{
					return 'keyword';
				}
			}
		}
		return 'text';
	}
	public function importData(): void
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$select = ['*'];
		$filter = 
		[
			'IBLOCK_ID' => $config['IBLOCK_ID_CATALOG'], 
			'ACTIVE' => 'Y', 
			'GLOBAL_ACTIVE' => 'Y'
		];
		$cnt = array('nTopCount' => 500000);
		$sections = CIBlockSection::GetList([], $filter, false, $select, $cnt);
		$cachedSectionFilter = $this->cachedSection();
		while($section = $sections->GetNext())
		{
			$tmpSection = [];
			$tmpSection = $section;
			foreach($tmpSection as $key => $sec)
			{
				if($key[0] == '~')
				{
					unset($tmpSection[$key]);
					continue;
				}
				if(($sec == '') || ($sec == null))
				{
					unset($tmpSection[$key]);
					continue;
				}
				$tmpSection[$key] = $sec;
			}
			// unset($tmpSection['DESCRIPTION']);
			// unset($tmpSection['SEARCHABLE_CONTENT']);
			unset($tmpSection['IBLOCK_TYPE_ID']);
			unset($tmpSection['IBLOCK_CODE']);
			unset($tmpSection['IBLOCK_EXTERNAL_ID']);
			unset($tmpSection['EXTERNAL_ID']);
			unset($tmpSection['TIMESTAMP_X']);
			unset($tmpSection['DATE_CREATE']);
			unset($tmpSection['CREATED_BY']);
			unset($tmpSection['IBLOCK_ID']);
			unset($tmpSection['SORT']);
			unset($tmpSection['LEFT_MARGIN']);
			unset($tmpSection['RIGHT_MARGIN']);
			unset($tmpSection['LIST_PAGE_URL']);

			$search = new Search();
			$tmpSection['SEARCHABLE_CONTENT'] = $search->customTranslit($tmpSection['SEARCHABLE_CONTENT']);


			foreach($cachedSectionFilter as $key => $val) 
				if($key == $tmpSection['XML_ID']) 
					$tmpSection['PROPS'] = $val;

			$tmpSection['PROPS']['CML2_BASE_UNIT'] = $this->getAddedSettings(127);
			$tmpSection['PROPS']['ARTCOIN_BONUS'] = $this->getAddedSettings(473);
			$tmpSection['PROPS']['ARTCOIN_DEBIT'] = $this->getAddedSettings(474);
			$tmpSection['PROPS']['EKSPOZITSIYA'] = $this->getAddedSettings(493);
			$tmpSection['PROPS']['DELIVERY_PERIOD'] = $this->getAddedSettings(978);
			$tmpSection['PROPS']['PRICE_8'] = 
			[
				"ID"=> "1", 
				"IBLOCK_ID"=> "2", 
				"NAME"=> "Стоимость", 
				"ACTIVE"=> "Y",
				"SORT"=> "1",
				"CODE"=> "PRICE_8",
				"PROPERTY_TYPE"=> "N",
				"LIST_TYPE"=> "L",
				"MULTIPLE"=> "N",
				"SEARCHABLE"=> "Y",
				"FILTRABLE"=> "Y",
				"HINT"=> "PRICE_8",
				"DISPLAY_EXPANDED"=> "Y",
				"DISPLAY_TYPE"=> "B"
			];

			$bulk = ['body' => []];
			$bulk['body'][] = 
			[
				'create' => 
				[
					'_index' => $config['INDEX_SECTIONS_NAME'], 
					'_id' => $section['ID']
				]
			];
			$bulk['body'][] = $tmpSection;
			$elastic->bulk($bulk);
			$section=[];
		}
	}
	public function getBxPropsSettings(): array
	{
		$result = [];
		$config = ElkClient::getConfig();
		$sort = array('sort' => 'asc', 'name' => 'asc');
		$filter = array('ACTIVE' => 'Y', 'IBLOCK_ID' => $config['IBLOCK_ID_CATALOG']);
		$properties = CIBlockProperty::GetList($sort, $filter);
		while($propFields = $properties->GetNext())
		{
			foreach($propFields as $k => $prop)
			{
				if($k[0] == '~') unset($propFields[$k]);
				if($k == 'TIMESTAMP_X') unset($propFields[$k]);
				if($k == 'ROW_COUNT') unset($propFields[$k]);
				if($k == 'COL_COUNT') unset($propFields[$k]);
				if($k == 'IS_REQUIRED') unset($propFields[$k]);
				if($k == 'VERSION') unset($propFields[$k]);
				if(!is_array($prop))
					if(($prop == '') || ($prop == null))
						unset($propFields[$k]);
			}
			$connection = \Bitrix\Main\Application::getConnection();
			$sql = 'select DISPLAY_EXPANDED,FILTER_HINT,DISPLAY_TYPE from b_iblock_section_property where (IBLOCK_ID=' . $config['IBLOCK_ID_CATALOG'] . ') AND (PROPERTY_ID=' . $propFields['ID'] . ')';
			$recordset = $connection->query($sql);
			$p = [];
			while($item = $recordset->fetch())
				$p = $item;
			if(($p['DISPLAY_EXPANDED'] != '') && ($p['DISPLAY_EXPANDED'] != null)) 
				$propFields['DISPLAY_EXPANDED'] = $p['DISPLAY_EXPANDED'];
			if(($p['FILTER_HINT'] != '') && ($p['FILTER_HINT'] != null)) 
				$propFields['FILTER_HINT'] = $p['FILTER_HINT'];
			if(($p['DISPLAY_TYPE'] != '') && ($p['DISPLAY_TYPE'] != null)) 
				$propFields['DISPLAY_TYPE'] = $p['DISPLAY_TYPE'];
			if($propFields['XML_ID'] != '') 
				$result[$propFields['XML_ID']] = $propFields;
		}
		return $result;
	}
	public function cachedSection(): array
	{
		$cache = $this->getCachedSections();  //[xml props] = [xml sections]
		$props = $this->getBxPropsSettings(); //[xml props] = [prop settings]
		$result = [];
		foreach($cache as $xmlProps => $xmlSections)
		{
			foreach($xmlSections as $section)
				$result[$section][$props[$xmlProps]['CODE']] = $props[$xmlProps];
		}
		return $result;
	}
	public function getCachedSections(): array
	{
		$result = [];
		$config = $this->getConfig();
		$select = array('NAME', 'DETAIL_TEXT',);
		$filter = array('IBLOCK_ID' => $config['IBLOCK_ID_CACHED_FILTER'], 'ACTIVE' => 'Y', 'GLOBAL_ACTIVE' => 'Y',);
		$cnt = array('nPageSize' => 500000);
		$bxCachedFilter = CIBlockElement::GetList(array('SORT' => 'ASC'), $filter, false, $cnt, $select);
		while($ob = $bxCachedFilter->GetNextElement())
		{
			$bxCachedFields = $ob->GetFields();
			$sectionXmlArr = explode(',', $bxCachedFields['DETAIL_TEXT']);
			$sectionXmlArr = array_diff($sectionXmlArr, array(''));
			$result[$bxCachedFields['NAME']] = $sectionXmlArr;
		}
		$result['PRICE_8'] = ['PROPERTY_TYPE' => 'N'];
		return $result;
	}
	public function getProizvoditelSettings(): array
	{
		$result = [];
		$config = $this->getConfig();
		$properties = CIBlockProperty::GetList(
			array('sort' => 'asc', 'name' => 'asc'), 
			array('ACTIVE' => 'Y', 'CODE' => 'PROIZVODITEL', 'IBLOCK_ID' => $config['IBLOCK_ID_CATALOG'])
		);
		while($propFields = $properties->GetNext())
		{
			foreach($propFields as $k => $prop)
			{
				if($k[0] == '~') unset($propFields[$k]);
				if($k == 'TIMESTAMP_X') unset($propFields[$k]);
				if($k == 'ROW_COUNT') unset($propFields[$k]);
				if($k == 'COL_COUNT') unset($propFields[$k]);
				if($k == 'IS_REQUIRED') unset($propFields[$k]);
				if($k == 'VERSION') unset($propFields[$k]);
				if(!is_array($prop)) if(($prop == '') | ($prop == null)) unset($propFields[$k]);
			}
			$result = $propFields;
		}
		return $result;
	}
	public function getAddedSettings($id): array
	{
		$result = [];
		$config = $this->getConfig();
		if(empty($id))
			$properties = CIBlockProperty::GetList(
				array('sort' => 'asc', 'name' => 'asc'), 
				array('ACTIVE' => 'Y', 'IBLOCK_ID' => $config['IBLOCK_ID_CATALOG'])
			);
		else
		$properties = CIBlockProperty::GetList(
			array('sort' => 'asc', 'name' => 'asc'), 
			array('ACTIVE' => 'Y', 'IBLOCK_ID' => $config['IBLOCK_ID_CATALOG'], "ID"=>$id) 
		);
		while($propFields = $properties->GetNext())
		{
			foreach($propFields as $k => $prop)
			{
				if($k[0] == '~') unset($propFields[$k]);
				if($k == 'TIMESTAMP_X') unset($propFields[$k]);
				if($k == 'ROW_COUNT') unset($propFields[$k]);
				if($k == 'COL_COUNT') unset($propFields[$k]);
				if($k == 'IS_REQUIRED') unset($propFields[$k]);
				if($k == 'VERSION') unset($propFields[$k]);
				if(!is_array($prop)) if(($prop == '') | ($prop == null)) unset($propFields[$k]);
			}
			$result = $propFields;
		}
		return $result;
	}
	public function setFilterData(): void
	{
		$result = [];
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$data = $this->getElasticSections();
		$data = $this->prepareFilterSections($data);
		foreach($data as $sectionId => $propertyData)
			$responseUpdate = $elastic->update(
				[
					'index' => 'termokit_sections', 
					'id' => (int)$sectionId, 
					'body' => 
					[
						'doc' => 
						[
							'FILTER' => json_encode($propertyData)
						]
					]
				]);

	}
	
	public function getElasticSections(): array
	{
		$result = [];
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$params = 
		[
			'index' => $config['INDEX_SECTIONS_NAME'],
			'body' => 
			[
				'query' => 
				[
					'bool' => 
					[
						'must' => 
						[
							'term' => 
							[
								'ACTIVE' => 'Y'
							]
						]
					]
				]
			],
			'size' => 500000
		];
		$response = $elastic->search($params);
		if($response['hits']['total']['value'] > 0)
		{
			$sections = $response['hits']['hits'];
			foreach($sections as $section)
				$result[$section['_source']['ID']] = $section['_source']['PROPS'];
		}
		return $result;
	}
	
	public function prepareFilterSections($sections): array
	{
		$result = [];
		foreach($sections as $id => $section)
		{
			foreach($section as $propertyCode => $property)
			{
				$elasticProps = $this->getElasticProps($id, $propertyCode, $property['PROPERTY_TYPE']);
				$result[] = $elasticProps;
			}
		}
		$t = [];
		foreach($result as $res)
			foreach($res as $sectionId => $arr)
				foreach($arr as $propertyCode => $property)
					$t[$sectionId][$propertyCode] = $property;
		return $t;
	}
	
	public function getElasticProps($sectionId, $propertyName, $propertyType): array
	{
		$result = [];
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$params = 
		[
			'index' => $config['INDEX_PRODUCTS_NAME'], 
			'body' => 
			[
				'query' => 
				[
					'bool' => 
					[
						'must' => 
						[
							[
								'match' => ['NAV_CHAIN_IDS' => (int)$sectionId]
							], 
							[
								'match' => ['ACTIVE' => 'Y']
							]
						]
					]
				]
			],
			'size' => 500000
		];
		$response = $elastic->search($params);
		if($response['hits']['total']['value'] > 0)
		{
			$products = $response['hits']['hits'];
			foreach($products as $product)
			{
				if($product['_source'][$propertyName] != '')
					$result[] = $product['_source'][$propertyName];
			}
		}
		$t = [];
		$result = array_count_values($result);
		foreach($result as $val => $cnt)
		{
			if($propertyType == 'L')
			{
				$property_enums = CIBlockPropertyEnum::GetList(
					array('DEF' => 'DESC', 'SORT' => 'ASC'), 
					array('IBLOCK_ID' => 2, 'CODE' => $propertyName, 'VALUE' => $val)
				);
				while($enum_fields = $property_enums->GetNext())
					$xml = $enum_fields['XML_ID'];
				$t[$xml] = array($val, $cnt);
			}
			else
			{
				$t[] = array($val, $cnt);
			}
		}
		$result = [];
		$result[$sectionId][$propertyName]['VALUES'] = $t;
		$result[$sectionId][$propertyName]['TYPE'] = $propertyType;
		return $result;
	}
	
	public function setSectionFilterJson(): void
	{
		$result = [];
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();

		$params = 
		[
			'index' => $config['INDEX_SECTIONS_NAME'], 
			'body' => 
			[
				'query' => 
				[
					'bool' => 
					[
						'must' => 
						[
							'term' => 
							[
								'ACTIVE' => 'Y'
							]
						]
					]
				]
			],
			'size' => 500000
		]; //, 'body' => ['query' => ['match_all' => []]]];
		$response = $elastic->search($params);
		if($response['hits']['total']['value'] > 0)
		{
			$sections = $response['hits']['hits'];
			foreach($sections as $section)
				$result[$section['_source']['ID']] = $section['_source']['CODE'];
		}
		foreach($result as $sectionId => $sectionCode)
		{
			$filter = $this->sectionProperties($sectionCode);
			$filter = json_encode($filter);
			$elastic->update(
				[
					'index' => $config['INDEX_SECTIONS_NAME'],
					'id' => (int)$sectionId,
					'body' => 
					[
						'doc' => 
						[
							'FILTER' => $filter
						]
					]
				]
			);
		}
	}
	
	public function sectionProperties($sectionCode = 'plitka'): array
	{
		$aggs = [];
		$config = ElkClient::getConfig();
		$sectionProps = $this->getSectionDataBySectionCode($sectionCode);
		foreach($sectionProps['PROPS'] as $code => $property)
		{
			if($property['PROPERTY_TYPE'] == 'L')
			{
				$aggs[$code] = ['terms' => ['field' => $code, 'size' => 500000]];
			}
			if($property['PROPERTY_TYPE'] == 'N')
			{
				$aggs[$code . '_MAX'] = ['max' => ['field' => $code]];
				$aggs[$code . '_MIN'] = ['min' => ['field' => $code]];
			}
		}
		$aggs['PROIZVODITEL'] = ['terms' => ['field' => 'PROIZVODITEL', 'size' => 500000]];
		$aggs['ARTCOIN_BONUS'] = ['terms' => ['field' => 'ARTCOIN_BONUS', 'size' => 500000]];
		$aggs['ARTCOIN_DEBIT'] = ['terms' => ['field' => 'ARTCOIN_DEBIT', 'size' => 500000]];
		$aggs['VES_KG_MAX'] = ['max' => ['field' => 'VES_KG']];
		$aggs['VES_KG_MIN'] = ['min' => ['field' => 'VES_KG']];
		$aggs['SHIRINA_MM_MAX'] = ['max' => ['field' => 'SHIRINA_MM']];
		$aggs['SHIRINA_MM_MIN'] = ['min' => ['field' => 'SHIRINA_MM']];
		$aggs['VYSOTA_MM_MAX'] = ['max' => ['field' => 'VYSOTA_MM']];
		$aggs['VYSOTA_MM_MIN'] = ['min' => ['field' => 'VYSOTA_MM']];
		$aggs['GLUBINA_MM_MAX'] = ['max' => ['field' => 'GLUBINA_MM']];
		$aggs['GLUBINA_MM_MIN'] = ['min' => ['field' => 'GLUBINA_MM']];
		$aggs['TOLSHCHINA_MM_MAX'] = ['max' => ['field' => 'TOLSHCHINA_MM']];
		$aggs['TOLSHCHINA_MM_MIN'] = ['min' => ['field' => 'TOLSHCHINA_MM']];
		$aggs['PRICE_8_MAX'] = ['max' => ['field' => 'PRICE_8']];
		$aggs['PRICE_8_MIN'] = ['min' => ['field' => 'PRICE_8']];
		$params = [
			'index' => $config['INDEX_PRODUCTS_NAME'],
			'body' => 
			[
				'query' => 
					[
						'bool' => 
							[
								'must' =>
									[
										[
											'match' => ['NAV_CHAIN_CODES' => $sectionCode]
										],
										[
											'match' => ['ACTIVE' => 'Y']
										],
									]
							]
					],
				'size' => 0,
				'aggs' => $aggs,
				'_source' => [false]
			]
		];
		$elastic = ElkClient::client();
		$responseAggs = $elastic->search($params);
		$resAggs = [];
		$resAggsN = [];
		foreach($responseAggs['aggregations'] as $code => $value)
		{
			if(substr($code, -3, 3) == 'MIN')
			{
				if($value['value'])
				{
					$resAggsN[str_replace('_MIN', '', $code)]['MIN'] = $value['value'];
				}
			}
			if(substr($code, -3, 3) == 'MAX')
			{
				if($value['value'])
				{
					$resAggsN[str_replace('_MAX', '', $code)]['MAX'] = $value['value'];
				}
			}
			if($value['buckets'])
			{
				$resAggs[$code] = $value['buckets'];
			}
		}
		$result = [];

		foreach($resAggs as $propCode => $val)
		{
			foreach($val as $k => $v)
			{
				$r = $this->getXmlProperty($propCode, $v['key']);
				if($r)
				{
					$result[$propCode]['VALS'][$r['XML_ID']]['VALUE'] = $v['key'];
					$result[$propCode]['SORT'] = $sectionProps['PROPS'][$propCode]['SORT'];
					$result[$propCode]['PROPERTY_TYPE'] = $sectionProps['PROPS'][$propCode]['PROPERTY_TYPE'];
					$result[$propCode]['LIST_TYPE'] = $sectionProps['PROPS'][$propCode]['LIST_TYPE'];
					$result[$propCode]['MULTIPLE'] = $sectionProps['PROPS'][$propCode]['MULTIPLE'];
					$result[$propCode]['NAME'] = $sectionProps['PROPS'][$propCode]['NAME'];
					$result[$propCode]['VALS'][$r['XML_ID']]['COUNT'] = $v['doc_count'];
				}
			}
		}
		if($resAggsN)
		{
			foreach($resAggsN as $k => $v)
			{
				$result[$k]= $sectionProps['PROPS'][$k];
				$result[$k]['VALS'] = $resAggsN[$k];
			}
		}
		return $result;
	}
	
	public function getSectionDataBySectionCode(string $sectionCode = ''): array
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$result = [];
		$params = [
			'index' => $config['INDEX_SECTIONS_NAME'], 
			'body' => 
			[
				'query' => 
				[
					'bool' => 
						[
							'must' => 
								[
									[
										'match' => ['CODE' => $sectionCode]
									],
									[
										'match' => ['ACTIVE' => 'Y']
									],
								]
						]
				]
			]
		];
		$response = $elastic->search($params);
		if($response['hits']['total']['value'] > 0)
			$result = $response['hits']['hits'][0]['_source'];
		return $result;
	}

	public function getXmlProperty($propertyCode, $value): array
	{
		$result = array();
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$params = 
		[
			'index' => $config['INDEX_PROPS_LIST'], 
			'body' => 
				[
					'size' => 10, 
					'query' => 
						[
							'bool' => 
								[
									'must' => 
										[
											'match' => ['PROPERTY_CODE' => $propertyCode], 
											'match' => ['VALUE' => $value]
										]
								]
						]
				]
		];
		$response = $elastic->search($params);
		if($response['hits']['total']['value'] > 0)
			$result = $response['hits']['hits'][0]['_source'];
		return $result;
	}

	public function getBxPropsValuesBySectionId(string $propType = '', string $propMultiple = '', int $propId = 0, int $sectionId = 0, int $iblockId = 0): array
	{
		$result = [];
		$filter = array('INCLUDE_SUBSECTIONS' => 'Y', 'ACTIVE' => 'Y', 'SECTION_ID' => $sectionId);
		$iterator = CIBlockElement::GetPropertyValues($iblockId, $filter, true, array('ID' => array($propId)));
		while($row = $iterator->Fetch())
		{
			/*
			PROPERTY_TYPE
			S - строка
			N - число
			F - файл
			L - список
			E - привязка к элементам
			G - привязка к группам.
			*/
			if($propType == 'S') $result[] = $row[$propId];
			if($propType == 'N') $result[] = $row[$propId];
			if($propType == 'L') $result = $row[$propId];
			if($propType == 'E') continue;
			if($propType == 'G') continue;
		}
		$result = array_count_values($result);
		return $result;
	}
	public function getPropsListBySectionId($sectionId, $propId): array
	{
		$config = ElkClient::getConfig();

		$result = [];
		$tmp = [];
		$id = [];
		$propType = 'L';
		$filter = array('INCLUDE_SUBSECTIONS' => 'Y', 'ACTIVE' => 'Y', 'GLOBAL_ACTIVE' => 'Y', 'SECTION_ID' => $sectionId);
		$iterator = CIBlockElement::GetPropertyValues($config['IBLOCK_ID_CATALOG'], $filter, true, array('ID' => array($propId)));
		while($row = $iterator->Fetch())
		{
			/*
				PROPERTY_TYPE
				S - строка
				N - число
				F - файл
				L - список
				E - привязка к элементам
				G - привязка к группам.
			*/

			if($propType == 'L')
			{
				$property_enums = CIBlockPropertyEnum::GetList(
					array('DEF' => 'DESC', 'SORT' => 'ASC'),
					array('IBLOCK_ID' => $config['IBLOCK_ID_CATALOG'], 
					'ID' => $row[$propId])
				);
				while($enum_fields = $property_enums->GetNext())
				{
					foreach($enum_fields as $k => $f)
						if(strpos($k, '~') !== false)
							unset($enum_fields[$k]);
					$tmp[$enum_fields['ID']] = $enum_fields;
					$id[] = $enum_fields['ID'];
				}
			}
		}
		$cnt = array_count_values($id);
		foreach($cnt as $id => $count)
		{
			$result[$id] = $tmp[$id];
			$result[$id]['COUNT'] = $count;
		}
		return $result;
	}

	public function sql($sql)
	{
		$connection = \Bitrix\Main\Application::getConnection();
		$sqlHelper = $connection->getSqlHelper();
		return $connection->query($sql);
	}
	
	public function is_empty(&$var)
	{
		return !($var || (is_scalar($var) && strlen($var)));
	}
	
	public function bigAgg($sectionCode): array
	{
		$props = $this->getSectionPropsByCode($sectionCode);
		$aggs = [];
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		foreach($props as $key => $val)
		{
			if($val['PROPERTY_TYPE'] == 'L')
				$aggs[$key] = ['terms' => ['field' => $key, 'size' => 500000]];
			if($val['PROPERTY_TYPE'] == 'N')
			{
				$aggs[$key . '_MAX'] = ['max' => ['field' => $key]];
				$aggs[$key . '_MIN'] = ['min' => ['field' => $key]];
			}
			$aggs['PROIZVODITEL'] = ['terms' => ['field' => 'PROIZVODITEL', 'size' => 500000]];
			$aggs['PRICE_8_MAX'] = ['max' => ['field' => 'PRICE_8']];
			$aggs['PRICE_8_MIN'] = ['min' => ['field' => 'PRICE_8']];
		}
		$params = 
		[
			'index' => $config['INDEX_PRODUCTS_NAME'], 
			'body' => 
				[
					'query' => 
						[
							'bool' => 
								[
									'must' => 
										[
											[
												'match' => ['NAV_CHAIN_CODES' => $sectionCode]
											], 
											[
												'match' => ['ACTIVE' => 'Y']
											],
										]
								]
						], 
					'size' => 0, 
					'aggs' => $aggs, 
					'_source' => [false]
				]
		];
		$responseAggs = $elastic->search($params);
		return $responseAggs;
	}
	
	public function getSectionPropsByCode($sectionCode): array
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$result = [];
		$params = 
		[
			'index' => $config['INDEX_SECTIONS_NAME'], 
			'body' => 
				[
					'query' => 
						[
							'bool' => 
								[
									'must' => 
										[
											[
												'match' => ['CODE' => $sectionCode]
											], 
											[
												'match' => ['ACTIVE' => 'Y']
											],
										]
								],
						]
				], 
			'_source' => ['PROPS']
		];
		$response = $elastic->search($params);
		return $response['hits']['hits'][0]['_source']['PROPS'];
	}
	
	public function getSectionFilterArrBySectionCode($sectionCode = 'plitka'): array
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$result = [];
		$params = 
		[
			'index' => $config['INDEX_SECTIONS_NAME'], 
			'_source' => ['FILTER'], 
			'body' => 
				[
					'query' => 
						[
							'bool' => 
								[
									'must' => 
										[
											[
												'match' => ['CODE' => $sectionCode]
											], 
											[
												'match' => ['ACTIVE' => 'Y']
											],
										]
								]
						]
				]
		];
		$response = $elastic->search($params);
		if($response['hits']['total']['value'] > 0)
			$result = $response['hits']['hits'][0]['_source'];
		$result = json_decode($result['FILTER'], true);
		if(empty($result))
			return false;
		else
			return $result;
	}
	
	public function removeSectionById($id): void
	{
		if($id)
		{
			$config = ElkClient::getConfig();
			$elastic = ElkClient::client();
			$params = 
			[
				'index' => $config['INDEX_SECTIONS_NAME'], 
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
														'ID' => $id
													]
												]
											]
									]
							]
					]
			];
			$response = $elastic->search($params);
			if($response['hits']['total']['value'] > 0)
				$elastic->delete(['index' => $config['INDEX_SECTIONS_NAME'], 'id' => $id]);
		}
	}
	
	public function getSectionData($sectionCode = 'plitka'): array
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$sectionProps = $this->getSectionDataBySectionCode($sectionCode);
		$params = 
		[
			'index' => $config['INDEX_SECTIONS_NAME'], 
			'body' => 
			[
				'query' => 
					[
						'bool' => 
							[
								'must' => 
									[
										[
											'match' => ['CODE' => $sectionCode]
										], 
										[
											'match' => ['ACTIVE' => 'Y']
										],
									]
							]
					],
				'size' => 1,
			]
		];
		$response = $elastic->search($params);
		$result = $response['hits']['hits'][0]['_source'];
		return $result;
	}
}