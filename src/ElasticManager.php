<?php
/*
 * Copyright (c) 2021, AAChibilyaev
 *
 * Chibilyaev Alexandr <info@aachibilyaev.com>
 *
 * AAChibilyaev LTD <https://aachibilyaev.com/>
 */
namespace aachibilyaev\bxelasticsearch;
class ElasticManager extends ElkClient
{
	public function __construct()
	{
		parent::__construct();
	}

	public function main()
	{
		$result = [];
		//$result['SECTION_DATA'] Возвращаем Данные категории и фильтр
		//$result['USER_FILTER'] Возвращаем фильтр пользователя(при наличии)
		//$result['PRODUCTS'] Возвращаем список товаров
		$url = str_replace('/elkcatalog','', $_SERVER['DOCUMENT_URI']);
		//$url = '/santekhnika/filter/proizvoditel-is-belbagno-or-1marka-or-astra-form-or-alvaro-banos-or-bravat-or-cezares-or-atoll-or-cersanit-or-bronze-de-luxe-or-alcaplast-or-bemeta-or-am-pm-or-aima-design-or-creo-ceramique-or-blanco-or-domani-spa-or-boheme-or-comforty-or-corozo-or-damixa-or-abber-or-d-k-or-deto-or-agger-or-cerastyle-or-berges-or-caprigo-or-ceramica-nova-or-ceruttispa-or-azario-or-boch-mann-or-bette-or-brabantia-or-acquabella-or-black-white-or-bas-or-axor-or-alex-baitler/ekspozitsiya-is-dmitriy-or-varshavka-or-moz/ves_kg-from-0-to-123123/apply/';
		$urlmanager = $this->urlManager($url);
		if ($urlmanager['STATUS'] == 2)
		{
            //active URL filter
			// Get Section CODE
			$result['SECTION_CODE'] = $this->getSectionCodeByUrlManager($urlmanager['URL_PARTS']);
			// Get Section DATA
			$result['SECTION_DATA'] = $this->getSectionDataBySectionCode($result['SECTION_CODE']);
			// !d($result['SECTION_DATA']);
			//Arr from url filter
			$filterFromUrl = $this->getArrayFromUrl($urlmanager['URL_PARTS']);
//			 !d($filterFromUrl);
			// !d($filterArr);
			// Set filter props UPPER CASE
			$filterFromUrl = $this->filterUpperCase($filterFromUrl);
//            !d($filterFromUrl);
			$result['FILTER_FROM_URL'] = $filterFromUrl;
//            !d($filterFromUrl);
            // Получаем значение свойств ТИПА СПИСОК для URL
            $filterFromUrl = $this->setArrFromFilterProps($filterFromUrl, $result['SECTION_DATA']);
//            !d($filterFromUrl);
            // Добавляем в свойства параметры из Catalog SECTION
			$uFilter = $this->makeUserFilter($filterFromUrl, $result['SECTION_DATA']['PROPS']);
//            !d($uFilter);
//            !d($result['SECTION_CODE']);
//            !d($result['SECTION_DATA']);
            $sectionFilter = new ElasticFilter();
//            $rr = $sectionFilter->getSectionFilter($result['SECTION_DATA']);
//            !d($rr);
            $result['FILTER'] = $sectionFilter->main($filterFromUrl, $result['SECTION_DATA']);
            die(1);
            //Создаем запрос для elasticsearch must term ..
//			$filterFromUrl = $this->makeElasticReqFromFilter($filterFromUrl, $result['SECTION_CODE']);
//			 !d($filterFromUrl);
            //Получаем товары по запросу
//			$products = $this->getProductsByElasticReq($filterFromUrl);
//			 !d($products);
            //Чистим товары от лишнего)
//			$products = $this->prepareResultProducts($products);
			// !d($products);
			// Отдает товары и базовую агрегацию КАТЕГОРИИ
//			$result['PRODUCTS'] = $products['PRODUCTS'];
//			$result['PRODUCTS_COUNT'] = $products['PRODUCTS_COUNT'];

			return $result;
			// d($result);
		}
	}

	public function makeUserFilter($filter, $sectionProps)
	{
		$tmp = $sectionProps;
		$result = [];
		foreach($filter as $code=>$f)
		{
			unset($tmp[$code]);
			$result[$code] = $sectionProps[$code];
            if($sectionProps[$code]['PROPERTY_TYPE'] == "L")
            {
                $listVal = [];
                if(!empty($f))
                    $result[$code]['USER_SELECT'] = $f;
//                    foreach ($f as $v)
//                        $listVal[] = $this->getPropertyDataByCodeAndValue($code,$v);
//                $result[$code]['USER_SELECT'] = $listVal;
            }
            else
            {
                $result[$code]['USER_SELECT'] = $f;
            }

		}
		foreach($tmp as $code=>$property)
		{
			$result[$code] = $property;
			$result[$code]['USER_SELECT'] = [];
		}
		return $result;
	}

	public function getSectionByCode(string $sectionCode = ''): array
	{
        $result= [];
		if ($sectionCode != '') {
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
									'match' => 
									[
										'CODE' => $sectionCode
									]
								]
							]
						]
					]
				]
			];
			$response = $elastic->search($params);
			if ($response['hits']['total']['value'] > 0) {
				$result = $response['hits']['hits'][0]['_source'];
			}
		}
		return $result;
	}

	public function getProductsBySectionCode(string $sectionCode = ''): array
	{
		if ($sectionCode != '') {
			$config = ElkClient::getConfig();
			$elastic = ElkClient::client();
			$result = [];
			/**
			 *  'sort' : [ { 'post_date' : {'order' : 'asc'}},]
			 */
			$sort = ['ID:desc'];
			$params = [
				'sort' => $sort, 
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
									'match' => 
									[
										'NAV_CHAIN_CODES' => $sectionCode
									]
								]
							]
						]
					]
				]
			];
			$response = $elastic->search($params);
			if ($response['hits']['total']['value'] > 0) {
				$result = $response['hits']['hits'];
			} else {
				$result = array();
			}
		} else {
			$result = array();
		}

		return $result;
	}
	
	public function getPropertyDataByCodeAndValue($code = '', $value = '')
	{
		$result = array();
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
								'term' => 
								[
									'PROPERTY_CODE' => $code
								]
							], 
							[
								'term' => 
								[
									'VALUE' => $value
								]
							],
						]
					]
				]
			],
		];
		$response = $elastic->search($params);
		if ($response['hits']['total']['value'] > 0)
			$result = $response['hits']['hits'][0]['_source'];
		return $result;
	}

	public function getPropertyTypeListByPropertyCodeAndValue(string $propertyCode, string $propertyValue): array
	{
		$result = array();
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
								'term' => 
								[
									'PROPERTY_CODE' => $propertyCode
								]
							], 
							[
								'term' => 
								[
									'VALUE' => $propertyValue
								]
							],
						]
					],
				]
			]
		];
		$response = $elastic->search($params);
		if ($response['hits']['total']['value'] > 0) {
			$result = $response['hits']['hits'][0]['_source'];
		} else {
			$result = array();
		}
		return $result;
	}

	public function getCountProductsBySectionByCode(string $sectionCode = ''): array
	{
		$result = array();
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$params = 
		[
			'index' => $config['INDEX_PRODUCTS_NAME'], 
			'_source' => false,
			'body' => 
			[
				'query' => 
				[
					'bool' => 
					[
						'must' => 
						[
							[
								'term' => 
								[
									'NAV_CHAIN_CODES' => $sectionCode
								]
							]
						]
					]
				]
			]
		];
		$response = $elastic->search($params);
		return $response['hits']['total']['value'];
	}
		
	public function getCountProductsFromSectionByCodeAndStore($sectionCode = '', $store = ''): array
	{
		$result = array();
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$params = 
		[
			'index' => $config['INDEX_PRODUCTS_NAME'], 
			'_source' => false, 
			'body' => 
			[
				'query' => 
				[
					'bool' => 
					[
						'must' => 
						[
							[
								'term' => 
								[
									'NAV_CHAIN_CODES' => $sectionCode
								]
							], 
							[
								'term' => 
								[
									'EKSPOZITSIYA' => $store
								]
							],
						]
					]
				]
			]
		];
		$response = $elastic->search($params);
		return $response['hits']['total']['value'];
	}

	public function getSectionDataBySectionCode(string $sectionCode = ''): array
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
								'match' => 
								[
									'CODE' => $sectionCode
								]
							]
						]
					]
				]
			]
		];
		$response = $elastic->search($params);
		if ($response['hits']['total']['value'] > 0) {
			$result = $response['hits']['hits'][0]['_source'];
		}
		return $result;
	}
	
	public function getPropsListByPropertyCodeAndXmlId($propertyCode, $xmlId)
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
								'term' => 
								[
									'PROPERTY_CODE' => $propertyCode
								]
							], 
							[
								'term' => 
								[
									'XML_ID' => $xmlId
								]
							],
						]
					],
				]
			],
		];
		$response = $elastic->search($params);
		if ($response['hits']['total']['value'] > 0)
			return $response['hits']['hits'][0]['_source'];
		else
			return false;
	}

	public function entryPoint($url)
	{
		$urlmanager = $this->urlManager($url);
		if ($urlmanager['STATUS'] == 1) 
		{
			//main catalog first page
		}
		if ($urlmanager['STATUS'] == 2) 
		{
			$result = [];	
			$resultAggs = [];
			// Get Section CODE
			$sectionCode = $this->getSectionCodeByUrlManager($urlmanager['URL_PARTS']); 
			// Get Section DATA
			$sectionData = $this->getSectionDataBySectionCode($sectionCode); 
			$result['SECTION_DATA'] = $sectionData;
			//Get arr from Url 
			$filterArr = $this->getArrayFromUrl($urlmanager['URL_PARTS']); 
			//Set filter props UPPER CASE
			$filterArr = $this->filterUpperCase($filterArr);

			$filter = $this->setArrFromFilterProps($filterArr, $sectionData); 
			$elasticReq = $this->makeElasticReqFromFilter($filter, $sectionCode);
			$sectionPropsAggregationRequest = $this->createAggregationRequestBySectionProps($sectionData['PROPS']);
			// Отдает товары и базовую агрегацию КАТЕГОРИИ
			$products = $this->getProductsByFilterAndSectionAggregation($elasticReq, $sectionPropsAggregationRequest);
			$arrAggsByFilter = $this->getAggsByFilter($filter, $sectionData['PROPS']);
			$resultAggregations = $this->getAggsResult($arrAggsByFilter, $sectionCode);
			$result['PRODUCTS'] = $products['hits']['hits'];
			$result['PRODUCTS_COUNT'] = $products['hits']['total']['value'];
			array_push($resultAggs, $resultAggregations['aggregations']);
			array_push($resultAggs, $products['aggregations']);
			$aggs = $this->prepareAggs($resultAggs, $sectionData['PROPS']);
			$result['FIILTERED_PROPS'] = $aggs;
			$result['USER_FILTER'] = $filter;
			$result['SECTION_DATA']['FILTER'] = json_decode($result['SECTION_DATA']['FILTER'], true);
			if ($urlmanager['URL_PARTS'][count($urlmanager['URL_PARTS']) - 2] == "action")
				$result['USER_FILTER']['ACTION'] = 1;
			return $result;
		}
		if ($urlmanager['STATUS'] == 3) 
		{
			//simple section
			$sectionCode = $urlmanager['URL_PARTS'];
			$sectionCode = $sectionCode[array_key_last($sectionCode)];
			$sectionData = $this->getSectionByCode($sectionCode);
			$productsData = $this->getProductsBySectionCode($sectionCode);
		}
	}
	//stay only List AND NUmber data
	//unset all filter data
	public function getAggsByFilter($filter, $sectionProps)
	{
		$req = [];
		foreach ($filter as $propCode => $propVals) 
		{
			if ( ($sectionProps[$propCode]['LIST_TYPE'] != 'L') || ($sectionProps[$propCode]['PROPERTY_TYPE'] == 'N') ) 
				continue;
			$tmp = $filter;
			unset($tmp[$propCode]);
			$req[$propCode] = $tmp;
		}
		return $req;
	}

	public function getAggsResult($filterProps, $sectionCode)
	{
		$result = [];
		foreach ($filterProps as $aggCode => $filter) 
		{
			$req = [];
			$req['query']['bool']['must']  = $this->makeElasticReqFromFilter($filter);
			$req['aggs'][$aggCode] = ['terms' =>['field' => $aggCode,'size' => 500000]];
			array_push($req['query']['bool']['must'], $this->getArrTerm(['CODE' => 'NAV_CHAIN_CODES', 'VALUE' => $sectionCode]));
			$tmp = $this->getElasticAggsResult($req);
			$result['aggregations'][$aggCode] = $tmp['aggregations'][$aggCode]; //['aggregations'][$aggCode]['buckets'];
		}
		return $result;
	}

	public function getElasticAggsResult($req)
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$params = 
		[
			'index' => $config['INDEX_PRODUCTS_NAME'], 
			'size' => 500000,
			'_source' => false,
			'body' => $req,
		];
		$response = $elastic->search($params);
		return $response;
	}

	public function prepareAggs($resultAggs, $sectionProps)
	{
		$result = array();
		foreach ($resultAggs as $item) 
		{
			foreach ($item as $propCode => $prop) 
			{
				if ( ($sectionProps[$propCode]['PROPERTY_TYPE'] == 'L'  ) && ($sectionProps[$propCode]['LIST_TYPE'] == 'L')) 
				{
					foreach ($prop['buckets'] as $k => $p) {
						$propS = $this->getPropertyDataByCodeAndValue($propCode, $p['key']);
						$result[$propCode]['VALS'][$propS['XML_ID']]['VALUE'] = $propS['VALUE'];
						$result[$propCode]['VALS'][$propS['XML_ID']]['COUNT'] = $p['doc_count'];
						$result[$propCode]['SORT'] = $sectionProps[$propCode]['SORT'];
						$result[$propCode]['PROPERTY_TYPE'] = $sectionProps[$propCode]['PROPERTY_TYPE'];
						$result[$propCode]['LIST_TYPE'] = $sectionProps[$propCode]['LIST_TYPE'];
						$result[$propCode]['MULTIPLE'] = $sectionProps[$propCode]['MULTIPLE'];
						$result[$propCode]['NAME'] = $sectionProps[$propCode]['NAME'];
					}
				}
				if(($sectionProps[$propCode]['PROPERTY_TYPE'] == 'S') && ($sectionProps[$propCode]['LIST_TYPE'] == 'L')  )
				{
					$i =0;
					foreach ($prop['buckets'] as $k => $p) {
						$result[$propCode]['VALS'][$i]['VALUE'] = $p['key'];
						$result[$propCode]['VALS'][$i]['COUNT'] = $p['doc_count'];
						
						$result[$propCode]['SORT'] = $sectionProps[$propCode]['SORT'];
						$result[$propCode]['PROPERTY_TYPE'] = $sectionProps[$propCode]['PROPERTY_TYPE'];
						$result[$propCode]['LIST_TYPE'] = $sectionProps[$propCode]['LIST_TYPE'];
						$result[$propCode]['MULTIPLE'] = $sectionProps[$propCode]['MULTIPLE'];
						$result[$propCode]['NAME'] = $sectionProps[$propCode]['NAME'];
						$i++;
					}
				}
			}
		}
		return $result;
	}

	public function urlManager($url)
	{
		$url = str_replace('/catalog/', '', $url);
		//		$url = str_replace('/elkcatalog/', '', $url);
		$urlParts = explode('/', $url);
		$urlParts = array_filter($urlParts, function ($element) { return !empty($element); });
		$status = 0;
		if (empty($urlParts)) 
		{
			//catalog main page
			$result['STATUS'] = 1;
			return $result;
		}
		else
		{
			if (array_search('filter', $urlParts) > 0) 
			{
				//filter
				$result['STATUS'] = 2;
				$result['URL_PARTS'] = $urlParts;
				return $result;
			} 
			else 
			{
				//simple section
				$result['STATUS'] = 3;
				$result['URL_PARTS'] = $urlParts;
				return $result;
			}
		}
	}

	public function getCatalogMain(): array
	{
		$result = array();
		$kernelSection = $this->getMainSection();

		if ($kernelSection) {
			foreach ($kernelSection as $k => $r) {
				$result[$k]['COUNT'] = $this->getCountProductsBySectionByCode($r['_source']['CODE']);
				$result[$k]['COUNT_DM'] = $this->getCountProductsFromSectionByCodeAndStore($r['_source']['CODE'], 'Дмитровское шоссе');
				$result[$k]['COUNT_VSH'] = $this->getCountProductsFromSectionByCodeAndStore($r['_source']['CODE'], 'Варшавское шоссе');
				$result[$k]['NAME'] = $r['_source']['NAME'];
				$result[$k]['CODE'] = $r['_source']['CODE'];
				$result[$k]['PICTURE'] = $r['_source']['PICTURE'];
				$result[$k]['SECTION_PAGE_URL'] = $r['_source']['SECTION_PAGE_URL'];
			}
		}
		return $result;
	}

	public function getMainSection(): array
	{
		$result = array();
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$params = 
		[
			'index' => $config['INDEX_SECTIONS_NAME'], 
			'_source' => 
			[
				'NAME', 'CODE', 'PICTURE', 'SECTION_PAGE_URL'
			], 
			'body' => 
			[
				'query' => 
				[
					'bool' =>
					[
						'must' => 
						[
							[
								'term' => 
								[
									'DEPTH_LEVEL' => 1
								]
							]
						]
					]
				]
			]
		];
		$response = $elastic->search($params);
		if ($response['hits']['total']['value'] > 0)
        {
			$result = $response['hits']['hits'];
		} else
        {
			$result = array();
		}
	}

	public function getSectionCodeByUrlManager(array $urlParts)
	{
		$category = '';
		$keyFilter = array_search('filter', $urlParts);
		if ($keyFilter > 0) {
			if ($urlParts[array_key_last($urlParts)] == 'apply') {
				if ($urlParts[array_key_last($urlParts) - 1] == 'clear') {
					//category
					//echo 'Очищенный фильтр - просто категория';
					$category = $urlParts[array_key_last($urlParts) - 3];
				} else {
					//filter
					//echo 'Тут надо фильтровать';
					$category = $urlParts[$keyFilter - 1];
				}
			}
		} else {
			//catalog/plitka/
			//	echo 'просто категория';
			$category = $urlParts[array_key_last($urlParts)];
		}
		return $category;
	}

	public function getArrayFromUrl(array $urlArr)
	{
		$urlArr = array_values($urlArr);
		if ($urlArr[count($urlArr) - 1] == 'apply') 
		{
			$keyFilter = array_search('filter', $urlArr);
			$section = $urlArr[$keyFilter - 1];
			$lastKey = count($urlArr) - 2;
			$startKey = $keyFilter + 1;
			while ($lastKey >= $startKey) 
			{
				$result[] = $urlArr[$startKey];
				$startKey++;
			}
		}
		$r = [];
		foreach ($result as $prop) 
		{
			if (substr_count($prop, '-or-') == 0) 
			{
				if (strpos($prop, '-from-')) 
				{
					$tmp = explode('-from-', $prop);
					$tmp1 = explode('-', $tmp[1]);
					$r[$tmp[0]]['MIN'] = $tmp1[0];
					if (strpos($tmp[1], '-to-')) 
					{
						$tmp2 = explode('-to-', $tmp[1]);
						$tmp3 = explode('-', $tmp2[1]);
						$r[$tmp[0]]['MAX'] = $tmp3[0];
					}
				}
				else 
				{
					if (strpos($prop, '-to-')) 
					{
						$tmp = explode('-to-', $prop);
						$tmp1 = explode('-', $tmp[1]);
						$r[$tmp[0]]['MAX'] = $tmp1[0];
					}
				}
				if (substr_count($prop, '-is-') >= 1) 
				{
					//тут только is
					$tmp = explode('-is-', $prop);
					$r[$tmp[0]] = $tmp[1];
					$tmp = '';
				}
			} 
			else
			{
				$tmpPropCode = explode('-is-', $prop);
				$propCode = $tmpPropCode[0];
				$tmpVals = explode('-or-', $tmpPropCode[1]);
				if(is_array($tmpVals))
					$r[$propCode] = $tmpVals;
				else
					$r[$propCode][] = $tmpVals;
			}
		}
		return $r;
	}

	//make filter props with UPPER CASE
	public function filterUpperCase($urlarr)
	{		
		$result = [];
		foreach ($urlarr as $key => $val) 
		{
			if (strstr($key, 'price')) 
			{
				$result['PRICE_8'] = $val;
				continue;
			}
			if (strstr($key, 'action')) 
			{
				$result['AAACTION'] = 1;
				continue;
			}
			$key = strtoupper($key);
			$result[$key] = $val;
		}
		return $result;
	}

	//prepare filter data from array
	public function setArrFromFilterProps($filter, $sectionData)
	{
		$result = [];
		foreach ($filter as $code => $arrVals) 
		{
			if ($sectionData['PROPS'][$code]['ID'])
			{
				if ($sectionData['PROPS'][$code]['PROPERTY_TYPE'] == 'L') 
				{
					$t = [];
					if (is_array($arrVals)) 
					{
						foreach ($arrVals as $codeVal) 
						{
                            //ТУТ ПРОДОЛЖИЬТ! НИХУЯ НЕТ СВОЙСТВА ЗАПОЛНЕННОГО
							if ($code == 'EKSPOZITSIYA')
							{
								$codeVal = ucfirst($codeVal);
							}
							$a = $this->getPropsListByPropertyCodeAndXmlId($code, $codeVal);
							if ($a['VALUE']) 
							{
								$t[] = $a['VALUE'];
							}
						}
					} 
					else 
					{
						if ($code == 'EKSPOZITSIYA') //ТУТ ПРОДОЛЖИЬТ! НИХУЯ НЕТ СВОЙСТВА ЗАПОЛНЕННОГО
						{
							$codeVal = ucfirst($arrVals);
						}
						$a = $this->getPropsListByPropertyCodeAndXmlId($code, $arrVals);
						if ($a['VALUE']) 
						{
							$t[] = $a['VALUE'];
						}
					}
					$result[$code] = $t;
				}
			  else if ($sectionData['PROPS'][$code]['PROPERTY_TYPE'] == 'S') 
			  {
				  $result[$code] = $arrVals;
			  }
			  else if($sectionData['PROPS'][$code]['PROPERTY_TYPE'] == 'N') 
			  {
				  if($arrVals['MIN'])
					$result[$code]['MIN'] = floatval($arrVals['MIN']);
				  if($arrVals['MAX'])
					$result[$code]['MAX'] = floatval($arrVals['MAX']);
			  }
			}
			else
			{
				/*
					Не найдено в свойствах категории в эластике
					echo '<pre>';
					print_r($code);
					print_r($arrVals);
					echo '</pre>';
					echo '<br/><br/> ИНАЧЕ!!$code  <br/><br/>';
				*/
			}
		}
		return $result;
	}

	public function getProductsByElasticReq($elasticReq)
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$req['query']['bool']['must'] = $elasticReq;
		$params = 
		[
			'index' => $config['INDEX_PRODUCTS_NAME'],
			'size' => 12,
			'body' => $req,
		];
		$response = $elastic->search($params);
		// !d($response);
		if ($response['hits']['total']['value'] > 0)
			$result = $response;
		else
			$result = -1;
		return $result;
	}

	public function prepareResultProducts($arr)
	{
		$result = [];
		$result['PRODUCTS_COUNT'] = $arr['hits']['total']['value'];
		if($result['PRODUCTS_COUNT'])
			foreach($arr['hits']['hits'] as $product)
				$result['PRODUCTS'][] = $product['_source'];
		return $result;
	}

	public function getProductsByFilterAndSectionAggregation($elasticReq, $sectionPropsAggregationRequest)
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$req['query']['bool']['must'] = $elasticReq;
		$req['aggs'] = $sectionPropsAggregationRequest['aggs'];
		$params = 
		[
			'index' => $config['INDEX_PRODUCTS_NAME'],
			'size' => 12,
			'body' => $req,
		];
		$response = $elastic->search($params);
		if ($response['hits']['total']['value'] > 0)
			$result = $response;
		else
			$result = [];
		return $result;
	}
	// section aggregation return elastic request width aggs without Number Props
	public function createAggregationRequestBySectionProps($sectionProps)
	{
		$req = [];
		foreach ($sectionProps as $key => $value) 
		{
			if ($value['PROPERTY_TYPE'] == 'N')
				{
				continue;
				}
			if (($value['LIST_TYPE'] == 'L') || ($value['PROPERTY_TYPE'] == 'L'))
				$req['aggs'][$key] = ['terms' => ['field' => $key]];
		}
		return $req;
	}

	public function makeElasticReqFromFilter($filter, $sectionCode = '')
	{
		$result = [];
		if (!empty($sectionCode))
			$result[] = $this->getArrTerm(['CODE' => 'NAV_CHAIN_CODES', 'VALUE' => $sectionCode]);
		
	   $result[] = $this->getArrTerm(['CODE' => 'ACTIVE', 'VALUE' => 'Y']);
		foreach ($filter as $code => $val)
		{
			if (is_array($val)) 
			{
				if ((array_key_exists('MIN', $val)) and (array_key_exists('MAX', $val))) 
				{
					$result[] = $this->getArrRange(['CODE' => $code, 'MIN' => $val['MIN'], 'MAX' => $val['MAX']]);
					continue;
				} 
				else if (array_key_exists('MIN', $val)) 
				{
					$result[] = $this->getArrRange(['CODE' => $code, 'MIN' => $val['MIN']]);
					continue;
				} 
				else if (array_key_exists('MAX', $val)) 
				{
					$result[] = $this->getArrRange(['CODE' => $code, 'MAX' => $val['MAX']]);
					continue;
				}
				if (count($val) == 1) 
				{
					$result[] = $this->getArrTerm(['CODE' => $code, 'VALUE' => $val[0]]);
				} 
				else 
				{	//IF LIST
					$rTmp = [];
					if (is_array($val)) 
					{
						if (!empty($va)) 
						{
							foreach ($val as $v) 
							{
								$l1 = $this->getPropsListByPropertyCodeAndXmlId($code, $v);
								if ($l1)
								{
									$rTmp[] = $this->getArrShould($l1['VALUE'], $code);
								}
							}
						}
					}
					if (!empty($rTmp)) 
					{
						$list = $this->getPropsListByPropertyCodeAndXmlId($code, $rTmp);
					} 
					else 
					{
						$result[] = $this->getArrShould($val, $code);
					}
				}
			} 
			else 
			{
				$list = $this->getPropsListByPropertyCodeAndXmlId($code, $val);
				if ($list) 
				{
					$result[] = $this->getArrTerm(['CODE' => $code, 'VALUE' => $list['VALUE']]);
				}
				else 
				{
					$result[] = $this->getArrTerm(['CODE' => $code, 'VALUE' => $val]);
				}
			}
		}
		return $result;
	}

	private function getArrTerm($arr): array
	{
		return ['term' => [$arr['CODE'] => $arr['VALUE']]];
	}

	private function getArrRange($arr): array
	{
		if (($arr['MIN'] > 0) and ($arr['MAX'] > 0)) {
			return ['bool' => ['must' => [['range' => [$arr['CODE'] => ['lte' => $arr['MAX'], 'gte' => $arr['MIN']]]]]]];
		}
		if ($arr['MIN'] > 0) {
			return ['bool' => ['must' => [['range' => [$arr['CODE'] => ['gte' => $arr['MIN']]]]]]];
		}
		if ($arr['MAX'] > 0) {
			return ['bool' => ['must' => [['range' => [$arr['CODE'] => ['lte' => $arr['MAX']]]]]]];
		}
	}

	private function getArrShould($arr, $code): array
	{
		$result = ['bool' => ['should' => []]];
		foreach ($arr as $key => $v) $result['bool']['should'][] = ['term' => [$code => $v]];
		return $result;
	}

	//Создание url из AJAX запроса! для Фильтра
	public function urlFromAjaxRequest(array $filter = [], $sectionCode='') // AJAX REQ -> URL
	{
		$section = $this->getSectionByCode($sectionCode);
		$url="";
		$urlArr=[];
		foreach ($filter as $propertyCode => $propertyValue) 
		{
			$url="";
			if(empty($propertyValue['VALUES'])) continue;
			//Если цена
			if ($propertyCode == 'PRICE_8') 
			{
				$url .= 'price-интернет';
				if (array_key_exists('MIN', $propertyValue['VALUES'])) 
				{
					if ($propertyValue['VALUES']['MIN']) 
					{
						$url .= '-from-' . $propertyValue['VALUES']['MIN'];
					}
				}
				if (array_key_exists('MAX', $propertyValue['VALUES'])) 
				{
					if ($propertyValue['VALUES']['MAX']) 
					{
						$url .= '-to-' . $propertyValue['VALUES']['MAX'];
					}
				}
				//$url .= '/';
				$urlArr[$section['PROPS'][$propertyCode]['SORT']] = $url;
				continue;
			}
			//Строка
			if ($section['PROPS'][$propertyCode]['PROPERTY_TYPE'] == 'S') 
			{
				$url .= strtolower($propertyCode) . '-is-' . $propertyValue['VALUES'] . '/';
			}
			//Число
			if ($section['PROPS'][$propertyCode]['PROPERTY_TYPE'] == 'N') 
			{
				$url .= strtolower($propertyCode);
				if (array_key_exists('MIN', $propertyValue['VALUES'])) 
				{
					if ($propertyValue['VALUES']['MIN']) 
					{
						$url .= '-from-' . $propertyValue['VALUES']['MIN'];
					}
				}
				if (array_key_exists('MAX', $propertyValue['VALUES'])) 
				{
					if ($propertyValue['VALUES']['MAX']) 
					{
						$url .= '-to-' . $propertyValue['VALUES']['MAX'];
					}
				}
				//$url .= '/';
				$urlArr[$section['PROPS'][$propertyCode]['SORT']] = $url;
			}
			//Список
			if ($section['PROPS'][$propertyCode]['PROPERTY_TYPE'] == 'L') 
			{
				$i = 0;
				if(is_array($propertyValue['VALUES']))
				{
					$url .= strtolower($propertyCode);
					foreach ($propertyValue['VALUES'] as $v) 
					{
						$propertyList = $this->getPropertyTypeListByPropertyCodeAndValue($propertyCode, $v);
						$v = mb_strtolower($propertyList['XML_ID']);
						if (!empty($v)) 
						{
							if ($i == 0) 
							{
								$url .= '-is-' . $v;
							} 
							else 
							{
								$url .= '-or-'.$v;
							}
							$i++;
						}
					}
				}
				else
				{
					$url .= strtolower($propertyCode) . '-is-' . $propertyValue['VALUES'];
				}
				//$url .= '/';
				$urlArr[$section['PROPS'][$propertyCode]['SORT']] = $url;
			}
		}

		ksort($urlArr);
		$result="/elkcatalog/".$sectionCode."/filter/";
		foreach($urlArr as $item)
			$result .= $item.'/';
		$result.="apply/";
		return $result;
	}

	//MAIN: AJAX POST REQUEST ->  ElasticProducts result
	public function getListProducts(array $filter = [])
	{
		$config = ElkClient::getConfig();
		$params = array();
		if (empty($filter['FILTER']['NAV_CHAIN_CODES'])) return 'error';
		if (empty($filter['SYSTEM']['INDEX'])) $filter['SYSTEM']['INDEX']  = $config['INDEX_PRODUCTS_NAME'];
		if (!empty($filter['SYSTEM']['FROM'])) $params['from'] = $filter['SYSTEM']['FROM']; else $params['from'] = 0;
		if (!empty($filter['SYSTEM']['SIZE'])) $params['size'] = $filter['SYSTEM']['SIZE']; else $params['size'] = $config['CATALOG_SECTION_COUNT'];
		if (!empty($filter['SYSTEM']['SORT'])) $params['sort'] = $filter['SYSTEM']['SORT'];
		if (!empty($filter['SYSTEM']['INDEX'])) $params['index'] = $filter['SYSTEM']['INDEX'];
		if (!empty($filter['SYSTEM']['SOURCE_EXCLUDE'])) $params['_source_excludes'] = $filter['SYSTEM']['SOURCE_EXCLUDE'];
		if (!empty($filter['SYSTEM']['SOURCE_INCLUDE'])) $params['_source_includes'] = $filter['SYSTEM']['SOURCE_INCLUDE'];
		if (!empty($filter['SYSTEM']['SOURCE'])) $params['_source'] = $filter['SYSTEM']['SOURCE'];
		if (!empty($filter['FILTER']))
		{
			foreach($filter['FILTER'] as $key=>$value)
			{
				if( (is_array($value)) AND (count($value)==1) )
				{
					$filter['FILTER'][$key]=$value[0];
				}
			}
			$params['body']['query']['bool']['must'] = $this->makeElasticReqFromFilter($filter['FILTER'], $filter['FILTER']['NAV_CHAIN_CODES']);
		}
		$elastic = ElkClient::client();
		$response = $elastic->search($params);
		$result = [];
		// print_r($response);
		if ($response['hits']['total']['value'] > 0)
			$result['PRODUCTS'] = $response['hits']['hits'];
			$result['COUNT'] = $response['hits']['total']['value'];
		return $result;
	}
	
	public function aggregationByFilterAndSectionCode($filter, $sectionCode)
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();

		$resultAggs = [];
		$result = [];	
		$sectionData = $this->getSectionDataBySectionCode($sectionCode); 
		$elasticReq = $this->makeElasticReqFromFilter($filter, $sectionCode);
		// print_r($elasticReq);
		$sectionPropsAggregationRequest = $this->createAggregationRequestBySectionProps($sectionData['PROPS']);
		//print_r($sectionPropsAggregationRequest);
		$products = $this->getProductsByFilterAndSectionAggregation($elasticReq, $sectionPropsAggregationRequest);
		//  print_r($products);
		$arrAggsByFilter = $this->getAggsByFilter($filter, $sectionData['PROPS']);
		// print_r($arrAggsByFilter);
		$resultAggregations = $this->getAggsResult($arrAggsByFilter, $sectionCode);
		//  print_r($resultAggregations);
		array_push($resultAggs, $resultAggregations['aggregations']);
		array_push($resultAggs, $products['aggregations']);
		//  print_r($resultAggs);
		$aggs = $this->prepareAggs($resultAggs, $sectionData['PROPS']);
		return $aggs;
	}

	public function newAgg($filter)
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$params = [];
		$params['index'] = $config['INDEX_PRODUCTS_NAME']; 
		$params['size'] = 0; 
		$params['_source'] = false; 
		$result = [];
		$sectionCode = 'plitka';

		$filterEmptyData = []; //Пустые значения 
		$filterData = []; //Фильтр пользователя с значениями
		$filterReqUser = []; //Elk запрос с пользовательскими данными

		//Разделяем пустые значения и пользовательские 
		foreach($filter as $item) { if(empty($item['VALUES'])) $filterEmptyData[] = $item; else $filterData[] = $item; }
		//Получение агрегации с пустыми пользовательскими фильтрами отфильтрованные по фильтру
		$filterReqUser = $this->filterRequestFromArrProperties($filterData);
		$params['body']['query']['bool']['must']=$filterReqUser;
		// $params['body']['query']['bool']['must']=['term' => ['ACTIVE' => 'Y']];
		foreach($filterEmptyData as $emptyProperty)
		{
			if(($emptyProperty['PROPERTY_TYPE']=='L') || ($emptyProperty['PROPERTY_TYPE']=='S'))
			{
					$params['body']['aggs'][$emptyProperty['CODE']] = ['terms' =>['field' => $emptyProperty['CODE'],'size' => 500000]];
			}
			else
			{
				$params['body']['aggs'][$emptyProperty['CODE'].'_MAX'] = [ 'max' =>['field' => $emptyProperty['CODE']] ];
				$params['body']['aggs'][$emptyProperty['CODE'].'_MIN'] = [ 'min' =>['field' => $emptyProperty['CODE']] ];
			}
		}
		// !d($params);
		//Считаем  агрегации по фильтру пользователя для полей без значений
		$response = $elastic->search($params);
		!d($response);
		$result['emptyFilterValues'] = $response;


		$params = [];
		$params['index'] = $config['INDEX_PRODUCTS_NAME']; 
		$params['size'] = 0; 
		$params['_source'] = false; 

		//Фильтр для выбранных полей - создание новых запросов 
		$filterRotate = [];
		foreach($filterData as $key => $v)
		{
			$tmp = $filterData;
			unset($tmp[$key]);
			$filterRotate[$v['CODE']] = $tmp;
		}
		// !d($filterRotate);
		foreach($filterRotate as $code =>$f)
		{
			if(($filter[$code]['PROPERTY_TYPE']=='L') || ($filter[$code]['PROPERTY_TYPE']=='S'))
			{
					$params['body']['aggs'][$code.'_agg']['filter']['bool']['must'] = $this->filterRequestFromArrProperties($f);
					$params['body']['aggs'][$code.'_agg']['aggs'][$code.'_subagg'] = ['terms' =>['field' => $code]];
			}
			if($filter[$code]['PROPERTY_TYPE']=='N')
			{
				$params['body']['aggs'][$code.'_MIN_agg']['filter']['bool']['must'] = $this->filterRequestFromArrProperties($f);
				$params['body']['aggs'][$code.'_MAX_agg']['filter']['bool']['must'] = $this->filterRequestFromArrProperties($f);
				$params['body']['aggs'][$code.'_MAX_agg']['aggs'][$code.'_subagg'] = [ 'max' =>['field' => $code] ];
				$params['body']['aggs'][$code.'_MIN_agg']['aggs'][$code.'_subagg'] = [ 'min' =>['field' => $code] ];
			}
		}
		$params['body']['query']['bool']['must']=['term' => ['ACTIVE' => 'Y']];
		// $params['body']['query']['bool']['must'][]=['bool' => ['term' => ['ACTIVE' => 'Y']]];
		// !d($params);
		$response = $elastic->search($params);
		// !d($response);
		$result['dataFilterValues'] = $response;
		$result = $this->prepareResultAggs($result);
		$result = $this->resultFilterListProps($filter, $result);
		return $result;
	}

	public function prepareResultAggs($filter)
	{
		$resultFilter=[];
		$emptyFilter = $filter['emptyFilterValues'];
		$dataFilter = $filter['dataFilterValues'];
		// !d($emptyFilter['aggregations']);
		
		foreach($emptyFilter['aggregations'] as $code=>$property)
		{
			if(stripos($code, '_MIN'))
			{
				$resultFilter[str_replace('_MIN','',$code)]['MIN'] = $property['value'];
			}
			else if(stripos($code, '_MAX') !== false)
			{
				$resultFilter[str_replace('_MAX','',$code)]['MAX'] = $property['value'];
			}
			else
			{
				foreach($property['buckets'] as $prop)
				{
					$resultFilter[$code]['VALUES'][] = $prop['key'];
					$resultFilter[$code]['COUNT'][] = $prop['doc_count'];
				}
				// $resultFilter[$code] = $property['buckets'];
			}
		}
		
		foreach($dataFilter['aggregations'] as $code=>$property)
		{
			$replaceCode = str_replace('_agg', '',$code);
			
			if(stripos($code, '_MIN_'))
			{
				$replaceCode = str_replace('_MIN', '',$replaceCode);
				$resultFilter[$replaceCode]['MIN'] = $property[$replaceCode.'_subagg']['value'];
			}
			else if(stripos($code, '_MAX_'))
			{
				$replaceCode = str_replace('_MAX', '',$replaceCode);
				$resultFilter[$replaceCode]['MAX'] = $property[$replaceCode.'_subagg']['value'];
			}
			else
			{
				foreach($property[$replaceCode.'_subagg']['buckets'] as $prop)
				{
					$resultFilter[$replaceCode]['VALUES'][] = $prop['key'];
					$resultFilter[$replaceCode]['COUNT'][] = $prop['doc_count'];
				}
				// $resultFilter[$code] = $property['buckets'];
			}
		
		}
		return $resultFilter;
	}

	//Переводим поля массива фильтра в запрос elk
	public function filterRequestFromArrProperties($filterData):array
	{
		$filterReqUser = [];
		foreach($filterData as $property)
		{
			if( ($property['PROPERTY_TYPE']=='S') || ($property['PROPERTY_TYPE']=='L') )
			{
				if(count($property['VALUES']) == 1 )
				{
					$filterReqUser[] = $this->getArrTerm(['CODE'=>$property['CODE'], 'VALUE'=>$property['VALUES']]);
				}
				if(count($property['VALUES']) > 1 )
				{
					$filterReqUser[] = $this->getArrShould($property['VALUES'], $property['CODE']);
				}
			}
			else if($property['PROPERTY_TYPE']=='N')
			{
				if ((array_key_exists('MIN', $property['VALUES'])) and (array_key_exists('MAX', $property['VALUES']))) 
				{
					$filterReqUser[] = $this->getArrRange(['CODE' => $property['CODE'], 'MIN' => $property['VALUES']['MIN'], 'MAX' => $property['VALUES']['MAX']]);
					continue;
				}
				else if (array_key_exists('MIN', $property['VALUES'])) 
				{
					$filterReqUser[] = $this->getArrRange(['CODE' => $property['CODE'], 'MIN' => $property['VALUES']['MIN']]);
					continue;
				}
				else if (array_key_exists('MAX', $property['VALUES'])) 
				{
					$filterReqUser[] = $this->getArrRange(['CODE' => $property['CODE'], 'MAX' => $property['VALUES']['MAX']]);
					continue;
				}
			}
		}
		return $filterReqUser;
	}

	public function resultFilterListProps($filter, $resultFilter)
	{
		foreach($resultFilter as $code=>$values)
		{
			if($filter[$code]['PROPERTY_TYPE'] == 'L')
			{
				foreach($values['VALUES'] as $val)
				{
						$p = $this->getPropertyDataByCodeAndValue($code,$val);
						$resultFilter[$code]['XML'][] = $p['XML_ID'] ;
				}
			}

		}

		return $resultFilter;
	}

	public function searchProducts($str)
	{
		$result = [];
		$search = new Search();
		$result = $search->searchProducts($str);
		$result = $this->getArrShould($result, 'ID');
		$result = $this->getProductsByElasticReq($result);
		$result = $this->prepareResultProducts($result);
		return $result;
	}

	public function searchSections($str)
	{
		$result = [];
		$search = new Search();
		$result = $search->searchSections($str);
		// $result = $this->getArrShould($result, 'ID');
		// $result = $this->getProductsByElasticReq($result);
		// $result = $this->prepareResultProducts($result);
		return $result;
	}

}