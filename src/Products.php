<?php

namespace aachibilyaev\bxelasticsearch;

use CIBlockElement;
use CCatalogProduct;
use CIBlockSection;
use CIBlockProperty;

class Products extends ElkClient
{
	public function __construct()
	{
		parent::__construct();
	}
	
	public function reCreateIndex()
	{
		$this->removeIndex();
		$this->createIndex();
	}
	
	public function removeIndex()
	{
        $config = ElkClient::getConfig();
        $elastic = ElkClient::client();
        if($elastic->indices()->exists(['index' => $config['INDEX_PRODUCTS_NAME']]))
            $elastic->indices()->delete(['index' => $config['INDEX_PRODUCTS_NAME']]);
	}
	
	public function createIndex()
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$elastic->indices()->create(['index' => $config['INDEX_PRODUCTS_NAME'], 'body' => ['settings' => array('number_of_shards' => 1, 'number_of_replicas' => 0, 'index.mapping.total_fields.limit' => 600000, 'index.max_result_window' => 500000, 'analysis' => ['analyzer' => ['my_analyzer' => ['tokenizer' => 'my_tokenizer']], 'tokenizer' => ['my_tokenizer' => ['type' => 'ngram', 'min_gram' => 3, 'max_gram' => 3, 'token_chars' => ['letter', 'digit']]]]),]]);
		$props = $this->iblockBitrixProps();
		$mappingData = $this->elasticMapping($props);
		$elastic->indices()->putMapping(['index' => $config['INDEX_PRODUCTS_NAME'], 'body' => $mappingData]);
		$products = $this->getBitrixProducts();
		$products = $this->prepareBitrixEmptyData($products);
		$result = $this->importProducts($products);
		return $result;
	}
	
	public function elasticMapping($props): array
	{
		$result = [];
		$config = ElkClient::getConfig();
		
		foreach ($props as $code => $prop) {
			if ($prop['PROPERTY_TYPE'] == 'N') 
			{
				// $bxPropValues = CIBlockElement::GetPropertyValues($config['IBLOCK_ID_CATALOG'], array('ACTIVE' => 'Y'), false, array('ID' => array($prop['ID'])));
				$type = 'float';
				// while ($bx = $bxPropValues->Fetch())
				// 	$type = $this->elasticTypeByValue($bx[$prop['ID']]);
			}
			if ($prop['PROPERTY_TYPE'] == 'F') 
			{
				$type = 'keyword';
			}
			if ($prop['PROPERTY_TYPE'] == 'E') 
			{
				$type = 'keyword';
			}
			if ($prop['PROPERTY_TYPE'] == 'G') 
			{
				$type = 'keyword';
			}
			if ($prop['PROPERTY_TYPE'] == 'S') 
			{
				$type = 'keyword';
			}
			if ($prop['PROPERTY_TYPE'] == 'L') 
			{
				$tmpVal = 'keyword';
				$type = $tmpVal;
			}
			$result['properties'][$code]['type'] = $type;
		}
		unset($result['properties']['IN_STOCK_TODAY_SPB']);
		unset($result['properties']['IN_STOCK_TOMORROW_SPB']);
		unset($result['properties']['IN_STOCK_AFTER_TOMORROW_SPB']);
		unset($result['properties']['IN_STOCK_WEEK_DAYS_SPB']);
		unset($result['properties']['FAST_DELIVERY_TODAY_SPB']);
		unset($result['properties']['ACTIVE_SPB']);
		unset($result['properties']['IS_AVAILABLE_SPB']);
		unset($result['properties']['DETAIL_PICTURE']);


		$result['properties']['ID']['type'] = 'integer';
		$result['properties']['ACTIVE']['type'] = 'keyword';
		$result['properties']['NAME']['type'] = 'keyword';
		$result['properties']['CODE']['type'] = 'keyword';
		$result['properties']['XML_ID']['type'] = 'keyword';
		$result['properties']['DETAIL_PAGE_URL']['type'] = 'keyword';
		$result['properties']['CANONICAL_PAGE_URL']['type'] = 'keyword';

		if ($result['properties']['PREVIEW_PICTURE']['type'])
			unset($result['properties']['PREVIEW_PICTURE']);
		if ($result['properties']['PREVIEW_PICTURE']['type'])
			unset($result['properties']['PREVIEW_PICTURE']);
		$result['properties']['PICTURES']['type'] = 'integer';
		$result['properties']['PREVIEW_TEXT']['type'] = 'keyword'; 
		$result['properties']['PRICE_8']['type'] = 'float';     //Интернет
		$result['properties']['OLD_PRICE']['type'] = 'float';     //Интернет
		//		$result['properties']['PRICE_13']['type'] = 'float'; //ЦенаБН
		//		$result['properties']['PRICE_15']['type'] = 'float'; //Оплата по картам
		//		$result['properties']['PRICE_21']['type'] = 'float'; //0
		//		$result['properties']['PRICE_22']['type'] = 'float'; //m
		//		$result['properties']['PRICE_23']['type'] = 'float'; //Зачеркнутые
		//		$result['properties']['PRICE_25']['type'] = 'float'; //Распродажа Москва
		//		$result['properties']['PRICE_28']['type'] = 'float'; //Залежавщийся
		//		$result['properties']['PRICE_29']['type'] = 'float'; //Цена по карте партнера
		//		$result['properties']['PRICE_30']['type'] = 'float'; //Интернет OZON
		//		$result['properties']['PRICE_ID_']['type'] = 'float'; //
		//Остатки
		$resStores = \CCatalogStore::GetList([], array('ACTIVE' => 'Y'), false, false, ['ID']);
		while ($arStore = $resStores->Fetch()) {
			if ($arStore['ID'] == 1) 
			{
				$result['properties']['STORE_' . $arStore['ID']]['type'] = 'integer';
			} //Остатки
		}

		$result['properties']['GROUP_IDS']['type'] = 'integer';
		$result['properties']['NAV_CHAIN_IDS']['type'] = 'integer';
		$result['properties']['GROUP_CODES']['type'] = 'keyword';
		$result['properties']['NAV_CHAIN_CODES']['type'] = 'keyword';
		$result['properties']['QUANTITY']['type'] = 'float';
		$result['properties']['DLINA_MM']['type'] = 'float';
		$result['properties']['VYSOTA_MM']['type'] = 'float';
		$result['properties']['SHIRINA_MM']['type'] = 'float';
		$result['properties']['VES_KG']['type'] = 'float';
		return $result;
	}
	
	public function iblockBitrixProps(): array // All iBlock Properties
	{
		$result = [];
		$config = ElkClient::getConfig();
		$properties = CIBlockProperty::GetList(array('sort' => 'asc', 'name' => 'asc'), array('ACTIVE' => 'Y', 'IBLOCK_ID' => $config['IBLOCK_ID_CATALOG']));
		while ($propFields = $properties->GetNext())
			$result[$propFields['CODE']] = $propFields;
		return $result;
	}
	
	public function elasticTypeByValue($propVal = '', $isFilter = 0, $isSearch = 0): string
	{
		$propVal = trim($propVal);
		$propValExplode = explode(' ', $propVal);
		if (count($propValExplode) < 5) 
		{
			if (is_bool($propVal)) 
			{
				return 'boolean';
			} 
			else 
			{
				if (is_numeric($propVal)) 
				{
					if (is_float($propVal)) 
					{
						return 'float';
					}
					if (is_int($propVal)) 
					{
						return 'integer';
					}
					if ($propVal == 1) 
					{
						return 'boolean';
					} 
					else 
					{
						if ($propVal == 0) 
						{
							return 'boolean';
						}
						else
						{
							return 'integer';
						}
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
	
	public function importProducts($products)
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		foreach ($products as $id => $product) 
		{
			$bulk = ['body' => []];
			$bulk['body'][] = ['create' => ['_index' => $config['INDEX_PRODUCTS_NAME'], '_id' => $id]];
			$bulk['body'][] = $product;
			$elastic->bulk($bulk);
		};
		return "";
	}
	
	public function getBitrixProducts($productId=0): array
	{
		$config = ElkClient::getConfig();
		$result = [];
		$select = array('*');	
		if($productId>0)
			$filter = array('IBLOCK_ID' => $config['IBLOCK_ID_CATALOG'], 'ID' => $productId); //, 'SECTION_ID' => 15885);//'SECTION_ID'=> 7809);
		else
			$filter = array('IBLOCK_ID' => $config['IBLOCK_ID_CATALOG'], 'INCLUDE_SUBSECTIONS' => 'Y', 'ACTIVE' => 'Y', 'GLOBAL_ACTIVE' => 'Y'); //, 'SECTION_ID' => 15885);//'SECTION_ID'=> 7809);
		$products = CIBlockElement::GetList(array('SORT' => 'ASC'), $filter, false, array('nPageSize' =>  5555555), $select);
		while ($obResult = $products->GetNextElement()) 
		{
			$fields = $obResult->GetFields();
			$properties = $obResult->GetProperties();
			$fields = $this->clearFields($fields);
			$result[$fields['ID']] = $fields;

			// if( ($productId>0) AND ($fields['ACTIVE'] !="Y")) 
			// return [];

			foreach ($properties as $property) 
			{
				if ($property['PROPERTY_TYPE'] == 'S') 
				{
					$result[$fields['ID']][$property['CODE']] = trim($property['VALUE']);
				}
				if ($property['PROPERTY_TYPE'] == 'L') 
				{
					if (is_array($property['VALUE_ENUM'])) 
					{
						$tEnum = [];
						foreach ($property['VALUE_ENUM'] as $a)
							$tEnum[] = trim($a);
						$result[$fields['ID']][$property['CODE']] = $tEnum;
					} 
					else 
					{
						$result[$fields['ID']][$property['CODE']] = trim($property['VALUE_ENUM']);
					}
				}
				if ($property['PROPERTY_TYPE'] == 'F') 
				{
					$result[$fields['ID']][$property['CODE']] = $property['VALUE'];
				}
				if ($property['PROPERTY_TYPE'] == 'E') 
				{
					$propEn = [];
					$selectEnother = array('ID', 'NAME');
					$filterEnother = array('IBLOCK_ID' => $property['LINK_IBLOCK_ID'], 'ACTIVE_DATE' => 'Y', 'ACTIVE' => 'Y', 'ID' => $property['~VALUE']);
					$resultEnother = CIBlockElement::GetList(array(), $filterEnother, false, array('nPageSize' => 600000), $selectEnother);
					while ($data = $resultEnother->GetNextElement()) 
					{
						$arFields = $data->GetFields();
						$propEn[] = $arFields['NAME'];
					}
					if (count($propEn) > 1) 
					{
						$result[$fields['ID']][$property['CODE']] = $propEn;
					} 
					else 
					{
						if (count($propEn) == 1) 
						{
							$result[$fields['ID']][$property['CODE']] = $propEn[0];
						}
					}
				}

				if ($property['PROPERTY_TYPE'] == 'N') 
				{
					if($property['VALUE'])
						$result[$fields['ID']][$property['CODE']] = floatval($property['VALUE']);
				}
				if($property['CODE'] == 'DSC_PRICE_VAL')
					if($property['VALUE']>0)
						$result[$fields['ID']]['OLD_PRICE'] =$property['VALUE'];
			}
			$prices = \Bitrix\Catalog\PriceTable::getList(['filter' => ['PRODUCT_ID' => $fields['ID'],]])->fetchAll();
			foreach ($prices as $price)
			{
				if ($price['CATALOG_GROUP_ID'] == 8) 
				{
					if ($price['PRICE'] > 0) {
						$result[$fields['ID']]['PRICE_8'] = $price['PRICE'];
					}
				}

				
				if ($price['CATALOG_GROUP_ID'] == 23) 
				{
					if ($price['PRICE'] > 0) 
					{
						if(empty($result[$fields['ID']]['OLD_PRICE']))
							$result[$fields['ID']]['OLD_PRICE'] = $price['PRICE'];
					}
				}

			}
			$storeProduct = \Bitrix\Catalog\StoreProductTable::getList(array('filter' => array('=PRODUCT_ID' => $fields['ID'], '=STORE.ACTIVE' => 'Y'), 'select' => array('AMOUNT', 'STORE_ID',),));
			while ($count = $storeProduct->fetch())
				if ($count['AMOUNT'] > 0) {
					$result[$fields['ID']]['STORE'][$count['STORE_ID']] = $count['AMOUNT'];
				}
			if (!array_key_exists('STORE', $result[$fields['ID']])) {
				foreach ($result[$fields['ID']]['PROPERTIES'] as $key => $val)
					if (!is_array($val)) {
						if ((trim($val) == '') || (empty($val))) {
							unset($result[$fields['ID']]['PROPERTIES'][$key]);
						} else {
							foreach ($val as $masKey => $masVal)
								if ((trim($masVal) == '') || (empty($masVal))) {
									unset($result[$fields['ID']]['PROPERTIES'][$key][$masKey]);
								}
							if (empty($result[$fields['ID']]['PROPERTIES'][$key])) {
								unset($result[$fields['ID']]['PROPERTIES'][$key]);
							}
						}
					}
			}
			$groups = [];
			$navChain = [];
			$rs = CIBlockElement::GetElementGroups($fields['ID']);
			while ($group = $rs->Fetch()) {
				$groups[] = $group;
				$navChainRs = CIBlockSection::GetNavChain($group['IBLOCK_ID'], $group['ID']);
				while ($chain = $navChainRs->Fetch())
					$navChain[] = $chain;
			}
			$result[$fields['ID']]['GROUP_IDS'] = array_map(function ($group) {return (int)$group['ID'];}, $groups);
			$result[$fields['ID']]['GROUP_CODES'] = array_values(array_filter(array_map(function ($group) {return $group['CODE'];}, $groups)));
			$result[$fields['ID']]['NAV_CHAIN_IDS'] = array_map(function ($group) {return (int)$group['ID'];}, $navChain);
			$result[$fields['ID']]['NAV_CHAIN_CODES'] = array_values(array_filter(array_map(function ($group) {return $group['CODE'];}, $navChain)));
			$ccCatalog = CCatalogProduct::GetByID($fields['ID']);
			$result[$fields['ID']]['QUANTITY'] = floatval($ccCatalog['QUANTITY']);
			$result[$fields['ID']]['DLINA_MM'] = floatval($ccCatalog['WIDTH']);
			$result[$fields['ID']]['VYSOTA_MM'] = floatval($ccCatalog['HEIGHT']);

			$result[$fields['ID']]['SHIRINA_MM'] = floatval($ccCatalog['LENGTH']);
			$result[$fields['ID']]['WEIGHT'] = floatval($ccCatalog['WEIGHT']);
		}
		return $result;
	}
	
	private function clearFields($fields = []): array
	{
		unset($fields['~ACTIVE']);
		unset($fields['~ID']);
		unset($fields['SORT']);
		// unset($fields['SEARCHABLE_CONTENT']);
		unset($fields['~SORT']);
		unset($fields['TIMESTAMP_X']);
		unset($fields['~TIMESTAMP_X']);
		unset($fields['TIMESTAMP_X_UNIX']);
		unset($fields['~TIMESTAMP_X_UNIX']);
		unset($fields['MODIFIED_BY']);
		unset($fields['~MODIFIED_BY']);
		unset($fields['DATE_CREATE']);
		unset($fields['~DATE_CREATE']);
		unset($fields['DATE_CREATE_UNIX']);
		unset($fields['~DATE_CREATE_UNIX']);
		unset($fields['CREATED_BY']);
		unset($fields['~CREATED_BY']);
		unset($fields['IBLOCK_ID']);
		unset($fields['~IBLOCK_ID']);
		unset($fields['IBLOCK_SECTION_ID']);
		unset($fields['~IBLOCK_SECTION_ID']);
		unset($fields['ACTIVE_FROM']);
		unset($fields['~ACTIVE_FROM']);
		unset($fields['ACTIVE_TO']);
		unset($fields['~ACTIVE_TO']);
		unset($fields['DATE_ACTIVE_FROM']);
		unset($fields['~DATE_ACTIVE_FROM']);
		unset($fields['DATE_ACTIVE_TO']);
		unset($fields['~DATE_ACTIVE_TO']);
		unset($fields['~NAME']);
		unset($fields['~PREVIEW_PICTURE']);
		unset($fields['DETAIL_TEXT_TYPE']);
		unset($fields['~SEARCHABLE_CONTENT']);
		unset($fields['DETAIL_TEXT']);
		unset($fields['~DETAIL_TEXT']);
		unset($fields['PREVIEW_TEXT']);
		unset($fields['~PREVIEW_TEXT']);
		unset($fields['PREVIEW_TEXT_TYPE']);
		unset($fields['~PREVIEW_TEXT_TYPE']);
		unset($fields['~DETAIL_TEXT_TYPE']);
		unset($fields['~DETAIL_PICTURE']);
		unset($fields['DETAIL_PICTURE']);
		unset($fields['PREVIEW_PICTURE']);
		unset($fields['DETAIL_TEXT']);
		unset($fields['~DETAIL_TEXT']);
		unset($fields['WF_STATUS_ID']);
		unset($fields['~WF_STATUS_ID']);
		unset($fields['WF_PARENT_ELEMENT_ID']);
		unset($fields['~WF_PARENT_ELEMENT_ID']);
		unset($fields['WF_LAST_HISTORY_ID']);
		unset($fields['~WF_LAST_HISTORY_ID']);
		unset($fields['WF_NEW']);
		unset($fields['~WF_NEW']);
		unset($fields['LOCK_STATUS']);
		unset($fields['~LOCK_STATUS']);
		unset($fields['WF_LOCKED_BY']);
		unset($fields['~WF_LOCKED_BY']);
		unset($fields['WF_DATE_LOCK']);
		unset($fields['~WF_DATE_LOCK']);
		unset($fields['WF_COMMENTS']);
		unset($fields['~WF_COMMENTS']);
		unset($fields['IN_SECTIONS']);
		unset($fields['~IN_SECTIONS']);
		unset($fields['SHOW_COUNTER']);
		unset($fields['~SHOW_COUNTER']);
		unset($fields['SHOW_COUNTER_START']);
		unset($fields['~SHOW_COUNTER_START']);
		unset($fields['SHOW_COUNTER_START_X']);
		unset($fields['~SHOW_COUNTER_START_X']);
		unset($fields['~CODE']);
		unset($fields['TAGS']);
		unset($fields['~TAGS']);
		unset($fields['~XML_ID']);
		unset($fields['EXTERNAL_ID']);
		unset($fields['~EXTERNAL_ID']);
		unset($fields['TMP_ID']);
		unset($fields['~TMP_ID']);
		unset($fields['USER_NAME']);
		unset($fields['~USER_NAME']);
		unset($fields['LOCKED_USER_NAME']);
		unset($fields['~LOCKED_USER_NAME']);
		unset($fields['CREATED_USER_NAME']);
		unset($fields['~CREATED_USER_NAME']);
		unset($fields['LANG_DIR']);
		unset($fields['~LANG_DIR']);
		unset($fields['LID']);
		unset($fields['~LID']);
		unset($fields['IBLOCK_TYPE_ID']);
		unset($fields['~IBLOCK_TYPE_ID']);
		unset($fields['IBLOCK_CODE']);
		unset($fields['~IBLOCK_CODE']);
		unset($fields['IBLOCK_NAME']);
		unset($fields['~IBLOCK_NAME']);
		unset($fields['IBLOCK_EXTERNAL_ID']);
		unset($fields['~IBLOCK_EXTERNAL_ID']);
		unset($fields['~DETAIL_PAGE_URL']);
		unset($fields['LIST_PAGE_URL']);
		unset($fields['~LIST_PAGE_URL']);
		unset($fields['~CANONICAL_PAGE_URL']);
		unset($fields['CREATED_DATE']);
		unset($fields['~CREATED_DATE']);
		unset($fields['BP_PUBLISHED']);
		unset($fields['~BP_PUBLISHED']);
		return $fields;
	}
	
	public function prepareBitrixEmptyData($data = array()): array
	{
		foreach ($data as $id => $product) 
		{
			if (($product['ACTION_1C'] == 68778) || (($product['SALE_OF_UNSOLD'] != 'Нет') && ($product['SALE_OF_UNSOLD'] > 0))) 
				$product['AAACTION'] = 1;
			
				$pictures=[];
			if(!empty($product['MORE_PHOTO'])) 
			{
				if(is_array($product['MORE_PHOTO']))
					foreach ($product['MORE_PHOTO'] as $p) {
						$pictures[] = $p;
					}
				else 
				{
				$pictures[] = $product['MORE_PHOTO'];
				}
				unset($data[$id]['MORE_PHOTO']);
			}
			
			if(!empty($product['DETAIL_PICTURE'])) {

				if(is_array($product['DETAIL_PICTURE']))
					foreach ($product['DETAIL_PICTURE'] as $p) {
						$pictures[] = $p;
					}
				else 
				{
					$pictures[] = $product['DETAIL_PICTURE'];
				}
				unset($data[$id]['DETAIL_PICTURE']);
			}
			
			if(!empty($product['PREVIEW_PICTURE'])) 
			{

				if(is_array($product['PREVIEW_PICTURE']))
					foreach ($product['PREVIEW_PICTURE'] as $p) {
						$pictures[] = $p;
					}
				else 
				{
					$pictures[] = $product['PREVIEW_PICTURE'];
				}
				unset($data[$id]['PREVIEW_PICTURE']);
			}
			
			foreach ($product as $key => $prop) 
			{
				if (!is_array($prop)) {
					if ((trim($prop) == '') || ($prop == null) || ($prop == '') || (empty($prop))) 
					{
						unset($data[$id][$key]);
					}
				}
			}
			$data[$id]['PICTURES'] = $pictures;
			if (array_key_exists('CML2_TAXES', $data[$id]))
				unset($data[$id]['CML2_TAXES']);
		}
		return $data;
	}
	
	public function runCron(): void
	{
		$productsIds = $this->getCronProductsIdsFromCronTime();
		foreach ($productsIds as $id) 
		{
			$this->removeProductById($id);
			// $this->importProducts($id);
			$products = $this->getBitrixProducts($id);
			$products = $this->prepareBitrixEmptyData($products);
			$result = $this->importProducts($products);
		}
	}
	
	//return products IDs 
	public function getCronProductsIdsFromCronTime(): array 
	{
		$config = ElkClient::getConfig();
		$tmpTime = date('Y-m-d H:i:s'); //2015-02-09 19:09:35
		$time = file_get_contents($config['CRON_PRODUCTS']);
		if (!$time) {
			file_put_contents($config['CRON_PRODUCTS'], $tmpTime);
			$time = $tmpTime;
		}
		$connection = \Bitrix\Main\Application::getConnection();
		$sql = "select ID,TIMESTAMP_X from b_iblock_element where (IBLOCK_ID='" . $config['IBLOCK_ID_CATALOG'] . "') and (ACTIVE='Y') and (TIMESTAMP_X > '" . $time . "') ORDER BY DATE_CREATE LIMIT " . $config['CRON_LIMIT_PRODUCTS_UPDATE'];
		$recordset = $connection->query($sql);
		$result = array();
		while ($item = $recordset->fetch()) 
		{
			if ($item['ID']) 
				$result[] = $item['ID'];
			$tmpTime = $item['TIMESTAMP_X']->format('Y-m-d H:i:s');
		}
		file_put_contents($config['CRON_PRODUCTS'], $tmpTime);
		return $result;
	}
	
	public function removeProductById($id): void
	{
		if ($id) {
			$config = ElkClient::getConfig();
			$elastic = ElkClient::client();
			$params = ['index' => $config['INDEX_PRODUCTS_NAME'], 'body' => ['query' => ['bool' => ['must' => [['match' => ['ID' => $id]]]]]]];
			$response = $elastic->search($params);
			if ($response['hits']['total']['value'] > 0) 
				$elastic->delete(['index' => $config['INDEX_PRODUCTS_NAME'], 'id' => $id]);
		}
	}

}
