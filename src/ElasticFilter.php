<?php
/*
 * Copyright (c) 2021, AAChibilyaev
 *
 * Chibilyaev Alexandr <info@aachibilyaev.com>
 *
 * AAChibilyaev LTD <https://aachibilyaev.com/>
 */
namespace aachibilyaev\bxelasticsearch;
class ElasticFilter extends ElkClient
{
    public function __construct()
    {
        parent::__construct();
    }
    public function main($filter, $sectionData)
    {
        echo "<pre>";
        $result = [];
        d($filter);
        d($sectionData);
        $sectionFilter = $this->getSectionFilter($sectionData);
        d($sectionFilter);
        $rotateFilter = $this->getRotateFilter($filter, $sectionData);
        $result = $this->prepareResult($sectionFilter, $rotateFilter, $sectionData);
        d($rotateFilter);

        $currentFilter= $this->getCurrentFilter($filter, $sectionData);
        d($currentFilter);

        return $result;
    }
    public function getSectionFilter($sectionData)
    {
        $config = ElkClient::getConfig();
        $elastic = ElkClient::client();
        $params = [];
        $result = [];
        $params['index'] = $config['INDEX_PRODUCTS_NAME'];
        $params['size'] = 300000;
        $params['_source'] = false;
        $params['body']['query']['bool']['must'][] = ['term' => ['ACTIVE' => 'Y']];
        $params['body']['query']['bool']['must'][] = ['term' => ['NAV_CHAIN_CODES' => $sectionData['CODE']]];
        foreach($sectionData['PROPS'] as $prop)
        {
                if(($prop['PROPERTY_TYPE']=='L') || ($prop['PROPERTY_TYPE']=='S'))
                {
                    $params['body']['aggs'][$prop['CODE']] = ['terms' =>['field' => $prop['CODE'], 'size'=>200000]];
                }
                else
                {
                    $params['body']['aggs'][$prop['CODE'].'_MAX'] = [ 'max' =>['field' => $prop['CODE']]];
                    $params['body']['aggs'][$prop['CODE'].'_MIN'] = [ 'min' =>['field' => $prop['CODE']]];
                }
        }
        //Считаем  агрегации по фильтру пользователя для полей без значений
        $r =  $elastic->search($params);
        foreach ($r['aggregations'] as $code=>$agg)
        {
            if (stripos($code, '_MIN'))
            {
                $result[str_replace('_MIN', '', $code)]['MIN'] = $agg['value'];
            }
            else if (stripos($code, '_MAX') !== false)
            {
                $result[str_replace('_MAX', '', $code)]['MAX'] = $agg['value'];
            }
            else
            {
                foreach ($agg['buckets'] as $prop)
                {
                    $tmp['VALUE'] = $prop['key'];
                    $tmp['COUNT'] = $prop['doc_count'];
                    $result[$code][] = $tmp;
                }
            }
        }
        return $result;
    }
    public function getRotateFilter($filter, $sectionData)
    {
        $result = [];
        $params = [];
        $config = ElkClient::getConfig();
        $elastic = ElkClient::client();
        $params['index'] = $config['INDEX_PRODUCTS_NAME'];
        $params['size'] = 500000;
        $params['_source'] = false;
        $params['body']['query']['bool']['must'][] = ['term' => ['ACTIVE' => 'Y']];
        $params['body']['query']['bool']['must'][] = ['term' => ['NAV_CHAIN_CODES' => $sectionData['CODE']]];
        $filterRotate = [];
        foreach ($filter as $key => $v)
        {
            $tmp = $filter;
            unset($tmp[$key]);
            $filterRotate[$key] = $tmp;
        }
        foreach ($filterRotate as $code => $f)
        {
            if (($sectionData['PROPS'][$code]['PROPERTY_TYPE'] == 'L') || ($sectionData['PROPS'][$code]['PROPERTY_TYPE'] == 'S'))
            {
                $params['body']['aggs'][$code . '_agg']['filter']['bool']['must'] = $this->makeElasticRequestFilter($f, $sectionData);
                $params['body']['aggs'][$code . '_agg']['aggs'][$code . '_subagg'] = ['terms' => ['field' => $code, 'size'=>200000]];
            }
            if ($sectionData['PROPS'][$code]['PROPERTY_TYPE'] == 'N')
            {
                $params['body']['aggs'][$code . '_MIN_agg']['filter']['bool']['must'] = $this->makeElasticRequestFilter($f, $sectionData);
                $params['body']['aggs'][$code . '_MIN_agg']['aggs'][$code . '_subagg'] = ['min' => ['field' => $code]];

                $params['body']['aggs'][$code . '_MAX_agg']['filter']['bool']['must'] = $this->makeElasticRequestFilter($f, $sectionData);
                $params['body']['aggs'][$code . '_MAX_agg']['aggs'][$code . '_subagg'] = ['max' => ['field' => $code]];
            }
        }
        $response = $elastic->search($params);
        if(!empty($response['aggregations']))
        {
            foreach ($response['aggregations'] as $code=>$agg)
            {
                if(substr_count($code, '_MIN_agg')>0)
                {
                    $propertyCode = str_replace('_MIN_agg','',$code);
                    if($agg['doc_count'])
                    {
                        $result[$propertyCode]['MIN']['COUNT'] = $agg['doc_count'];
                    }
                    if($agg[$propertyCode.'_subagg']['value'])
                    {
                        $result[$propertyCode]['MIN']['VALUE'] = $agg[$propertyCode.'_subagg']['value'];
                    }
                }
                else if(substr_count($code, '_MAX_agg')>0)
                {
                    $propertyCode = str_replace('_MAX_agg','',$code);
                    if($agg['doc_count'])
                    {
                        $result[$propertyCode]['MAX']['COUNT'] = $agg['doc_count'];
                    }
                    if($agg[$propertyCode.'_subagg']['value'])
                    {
                        $result[$propertyCode]['MAX']['VALUE'] = $agg[$propertyCode.'_subagg']['value'];
                    }
                }
                else
                {
                    $propertyCode = str_replace('_agg','',$code);
                    if(!empty($agg[$propertyCode.'_subagg']['buckets']))
                    {
                        foreach ($agg[$propertyCode.'_subagg']['buckets'] as $props)
                        {
                            $tmp = [];
                            $tmp['COUNT'] = $props['key'];
                            $tmp['VALUE'] = $props['doc_count'];
                            $result[$propertyCode][] = $tmp;
                        }
                    }
                }

            }
        }
        return $result;
    }
    public function getCurrentFilter($filter, $sectionData)
    {
        $config = ElkClient::getConfig();
        $elastic = ElkClient::client();
        $params = [];
        $result = [];
        $params['index'] = $config['INDEX_PRODUCTS_NAME'];
        $params['size'] = 300000;
        $params['_source'] = false;
        $params['body']['query']['bool']['must'] = $this->makeElasticRequestFilter($filter, $sectionData);
        $params['body']['query']['bool']['must'][] = ['term' => ['ACTIVE' => 'Y']];
        $params['body']['query']['bool']['must'][] = ['term' => ['NAV_CHAIN_CODES' => $sectionData['CODE']]];

        foreach($sectionData['PROPS'] as $prop)
        {
            if(($prop['PROPERTY_TYPE']=='L') || ($prop['PROPERTY_TYPE']=='S'))
            {
                $params['body']['aggs'][$prop['CODE']] = ['terms' =>['field' => $prop['CODE'], 'size'=>200000]];
            }
            else
            {
                $params['body']['aggs'][$prop['CODE'].'_MAX'] = [ 'max' =>['field' => $prop['CODE']]];
                $params['body']['aggs'][$prop['CODE'].'_MIN'] = [ 'min' =>['field' => $prop['CODE']]];
            }
        }
        //Считаем  агрегации по фильтру пользователя для полей без значений
        $r =  $elastic->search($params);
        foreach ($r['aggregations'] as $code=>$agg)
        {
            if (stripos($code, '_MIN'))
            {
                $result[str_replace('_MIN', '', $code)]['MIN'] = $agg['value'];
            }
            else if (stripos($code, '_MAX') !== false)
            {
                $result[str_replace('_MAX', '', $code)]['MAX'] = $agg['value'];
            }
            else
            {
                foreach ($agg['buckets'] as $prop)
                {
                    $tmp['VALUE'] = $prop['key'];
                    $tmp['COUNT'] = $prop['doc_count'];
                    $result[$code][] = $tmp;
                }
            }
        }
        return $result;
    }
    public function prepareResult($sectionFilter, $propsFilter, $sectionData)
    {
        return "";
    }

    public function makeElasticRequestFilter($filter, $sectionData): array
    {
        $filterReqUser = [];
        foreach ($filter as $code=>$property)
        {
            if (($sectionData['PROPS'][$code]['PROPERTY_TYPE'] == 'S') || ($sectionData['PROPS'][$code]['PROPERTY_TYPE'] == 'L'))
            {
                if (count($property) == 1)
                {
                    $filterReqUser[] = $this->getArrTerm(['CODE' => $code, 'VALUE' => $property[0]]);
                }
                if (count($property) > 1)
                {
                    if($sectionData['PROPS'][$code]['PROPERTY_TYPE'] == 'S') {

                        $filterReqUser[] = $this->getArrShouldS($property, $code);
                    }
                    else
                    {
                        $filterReqUser[] = $this->getArrShould($property, $code);
                    }
                }
            }
            else if ($sectionData['PROPS'][$code]['PROPERTY_TYPE'] == 'N')
            {
                if ((array_key_exists('MIN', $property)) and (array_key_exists('MAX', $property)))
                {
                    $filterReqUser[] = $this->getArrRange(['CODE' => $code, 'MIN' => $property['MIN'], 'MAX' => $property['MAX']]);
                    continue;
                }
                else if (array_key_exists('MIN', $property['USER_SELECT']))
                {
                    $filterReqUser[] = $this->getArrRange(['CODE' => $code, 'MIN' => $property['MIN']]);
                    continue;
                }
                else if (array_key_exists('MAX', $property['USER_SELECT']))
                {
                    $filterReqUser[] = $this->getArrRange(['CODE' => $code, 'MAX' => $property['MAX']]);
                    continue;
                }
            }
        }
        return $filterReqUser;
    }
    private function getArrTerm($arr): array
    {
        return ['term' => [$arr['CODE'] => $arr['VALUE']]];
    }
    private function getArrRange($arr): array
    {
        if (($arr['MIN'] > 0) and ($arr['MAX'] > 0)) return ['bool' => ['must' => [['range' => [$arr['CODE'] => ['lte' => $arr['MAX'], 'gte' => $arr['MIN']]]]]]];
        if ($arr['MIN'] > 0) return ['bool' => ['must' => [['range' => [$arr['CODE'] => ['gte' => $arr['MIN']]]]]]];
        if ($arr['MAX'] > 0) return ['bool' => ['must' => [['range' => [$arr['CODE'] => ['lte' => $arr['MAX']]]]]]];
    }
    private function getArrShould($arr, $code): array
    {
        foreach ($arr as $v)
            $result['bool']['should'][] = ['term' => [$code => $v]];
        return $result;
    }
    private function getArrShouldS($arr, $propertyCode): array
    {
        foreach ($arr as $v)
            $result['bool']['should'][] = ['term' => [$propertyCode => $v['VALUE']]];
        return $result;
    }
}
