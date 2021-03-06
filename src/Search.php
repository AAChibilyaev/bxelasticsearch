<?php
/*
 * Copyright (c) 2021, AAChibilyaev
 *
 * Chibilyaev Alexandr <info@aachibilyaev.com>
 *
 * AAChibilyaev LTD <https://aachibilyaev.com/>
 */

namespace aachibilyaev\bxelasticsearch;

class Search extends ElkClient
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
		$elastic->indices()->delete(['index' => $config['INDEX_SEARCH_NAME']]);
	}
	
	public function createIndex(): void
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$elastic->indices()->create(
            [
                'index' => $config['INDEX_SEARCH_NAME'], 
                'body' => 
                [
                    'settings' => 
                    [
                        'number_of_shards' => 1, 
                        'number_of_replicas' => 0,
                        'index.mapping.total_fields.limit' => 600000,
                        'index.max_result_window' => 500000,
                        'analysis' => 
                        [
                            'analyzer' => 
                            [
                                'aac_search_analyzer' => 
                                [
                                    "type" => "custom",
                                    // 'tokenizer' => 'my_tokenizer',
                                    "tokenizer"=> "standard",
                                    "filter"=> [
                                        "lowercase",
                                        "search_synonym",
                                        // "ru_RU",
                                        //"russian_morphology",
                                        // "english_morphology",
                                        "ru_stopwords"
                                    ],
                                ]
                            ],
                            "filter" => 
                            [
                                "search_synonym" => 
                                [
                                    "ignore_case" => "true",
                                    "type" => "synonym",
                                    "synonyms" => 
                                    [
                                        "????????????,????????????????",
                                        "????????????,????????????"
                                    ]
                                ],
                                "ru_stopwords"=> 
                                [
                                    "type"=> "stop",
                                    "stopwords"=> "??,??????,??????????,????,??????,????????,????????,????????,????????,??,??????,??????,????????,????,??????,??????,??????????,????????,????,??????,????,????????,??????,????,??????,????,????????,????????,??????,????,????,??????????,??,????,??????,????,????,??,??????,????,??????????,??????,????,????????,??????,??????????,????,????,????????,??????,????,????????,??????,??????,????,??????,????,????,??,????,????????????,????,??????,??????,??????,????,??????????,????,??????,??????,??,????,??????,??????????,??????????,??????,????,??????,????,????????,????????,??????,????????????,??????,????,??,??????,????????,????????,??????,??????,??????,??????????,??????,??????,??????,??????,??????,??,a,an,and,are,as,at,be,but,by,for,if,in,into,is,it,no,not,of,on,or,such,that,the,their,then,there,these,they,this,to,was,will,with"
                                ],
                               

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
                                    
                    ],
                ]
            ]);
		$mappingData = $this->elasticMapping();
        $elastic->indices()->putMapping(['index' => $config['INDEX_SEARCH_NAME'], 'body' => $mappingData]);
        $this->updateIndex();
	}
    
    public function elasticMapping(): array
	{
		$result = [];
        $result['properties']['product_id']['type'] = 'integer';
        $result['properties']['search_data']['type'] = 'text';
        $result['properties']['search_data']['analyzer'] = 'aac_search_analyzer';
		return $result;
	}

    public function updateIndex()
    {
        $result = [];
        /*
            $product['NAME']
            $product['CODE']
        */
        $products = $this->getProducts();
        $searchableProps = $this->getSearchableProps();
        $result = $this->prepareSearchProps($products, $searchableProps);
        $result = $this->setData($result);
    }

    public function getSearchableProps()
    {
        $result = [];
        $result[] = "NAME";
        $result[] = "CODE";
        $result[] = "SEARCHABLE_CONTENT";
		$config = ElkClient::getConfig();
        $elastic = ElkClient::client();
        $params = [
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
                                                    'SEARCHABLE' => 'Y'
                                                ]
                                        ]
                                    ]
                            ]
                    ]
            ],
            'size' => 11222
        ];
        $response = $elastic->search($params);
        foreach($response['hits']['hits'] as $item)
            $result[] = $item['_source']['CODE'];
        return $result;
    }

    public function getProducts()
    {
        $result = [];
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
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
                                            'match' => 
                                                [
                                                    'ACTIVE' => 'Y'
                                                ]
                                        ]
                                    ]
                            ]
                    ]
            ],
            'size' => 500000
        ];
        $response = $elastic->search($params);
        foreach($response['hits']['hits'] as $item)
            $result[] = $item['_source'];
        return $result;
    }

    public function prepareSearchProps($products, $props)
    {
        $result = [];
        foreach($products as $product)
        {
            foreach($props as $prop)
            {
                if($product[$prop])
                {
                    $result[$product['ID']] = $this->customTranslit($product[$prop]);
                    // $result[$product['ID']] .= mb_strtolower($product[$prop])." ";
                    // $result[$product['ID']] .= mb_strtolower($this->simpleTranslit($product[$prop]))." ";
                    // $result[$product['ID']] .= mb_strtolower($this->myTranslit($product[$prop]));
                }    
            }
        }
        return $result;
    }

    public function customTranslit($str)
    {
        $result = "";
        $result .= mb_strtolower($str)." ";
        $result .= mb_strtolower($this->simpleTranslit($str))." ";
        $result .= mb_strtolower($this->myTranslit($str))." ";
        return $result;
    }

    private function simpleTranslit($value)
    {
	    $converter = array(
		'??' => 'a',    '??' => 'b',    '??' => 'v',    '??' => 'g',    '??' => 'd',
		'??' => 'e',    '??' => 'e',    '??' => 'zh',   '??' => 'z',    '??' => 'i',
		'??' => 'y',    '??' => 'k',    '??' => 'l',    '??' => 'm',    '??' => 'n',
		'??' => 'o',    '??' => 'p',    '??' => 'r',    '??' => 's',    '??' => 't',
		'??' => 'u',    '??' => 'f',    '??' => 'h',    '??' => 'c',    '??' => 'ch',
		'??' => 'sh',   '??' => 'sch',  '??' => '',     '??' => 'y',    '??' => '',
		'??' => 'e',    '??' => 'yu',   '??' => 'ya',
 
		'??' => 'A',    '??' => 'B',    '??' => 'V',    '??' => 'G',    '??' => 'D',
		'??' => 'E',    '??' => 'E',    '??' => 'Zh',   '??' => 'Z',    '??' => 'I',
		'??' => 'Y',    '??' => 'K',    '??' => 'L',    '??' => 'M',    '??' => 'N',
		'??' => 'O',    '??' => 'P',    '??' => 'R',    '??' => 'S',    '??' => 'T',
		'??' => 'U',    '??' => 'F',    '??' => 'H',    '??' => 'C',    '??' => 'Ch',
		'??' => 'Sh',   '??' => 'Sch',  '??' => '',     '??' => 'Y',    '??' => '',
		'??' => 'E',    '??' => 'Yu',   '??' => 'Ya',
        );
    
        $value = strtr($value, $converter);
        return $value;
    }
   
    private function myTranslit($value)
    {
	    $converter = array(
		'??' => 'f',    '??' => ',',    '??' => 'd',    '??' => 'u',    '??' => 'l',
		'??' => 't',    '??' => '`',    '??' => ';',   '??' => 'p',    '??' => 'b',
		'??' => 'q',    '??' => 'r',    '??' => 'k',    '??' => 'v',    '??' => 'y',
		'??' => 'j',    '??' => 'g',    '??' => 'h',    '??' => 'c',    '??' => 'n',
		'??' => 'e',    '??' => 'a',    '??' => '[',    '??' => 'w',    '??' => 'x',
		'??' => 'i',   '??' => 'o',  '??' => 'm',     '??' => 's',    '??' => ']',
		'??' => '',    '??' => '.',   '??' => 'z',
 
		'??' => 'F',    '??' => '<',    '??' => 'D',    '??' => 'U',    '??' => 'L',
		'??' => 'T',    '??' => '~',    '??' => ':',   '??' => 'P',    '??' => 'B',
		'??' => 'Q',    '??' => 'R',    '??' => 'K',    '??' => 'V',    '??' => 'Y',
		'??' => 'J',    '??' => 'G',    '??' => 'H',    '??' => 'C',    '??' => 'N',
		'??' => 'E',    '??' => 'A',    '??' => '{',    '??' => 'W',    '??' => 'X',
		'??' => 'I',   '??' => 'O',  '??' => 'M',     '??' => 'S',    '??' => '}',
		'??' => '',    '??' => '>',   '??' => 'Z',
        );
    
        $value = strtr($value, $converter);
        return $value;
    }
 
    private function setData($arr)
    {
        $config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		foreach ($arr as $id => $search_data) 
		{
			$bulk = ['body' => []];
			$bulk['body'][] = ['create' => ['_index' => $config['INDEX_SEARCH_NAME'], '_id' => $id]];
			$bulk['body'][] = ['search_data'=>$search_data , 'product_id'=>$id];
			$elastic->bulk($bulk);
		};
		return "";
    }

    //Entry point
    public function searchProducts($str)
    {
        $result = [];
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
        $params = 
        [
            'index' => $config['INDEX_SEARCH_NAME'],
            'body'  => 
            [
                'query' => 
                [
                    'match' => 
                    [
                        'search_data' => $str
                    ]
                ]
                // 'query' => 
                // [
                //     'bool'=>
                //     [
                //         'match' => 
                //         [
                //             'search_data' => $str
                //         ]
                //     ],
                //      "filter" => 
                //      [
                //          "range" => 
                //          [
                //              "product_id" => [ "gt" =>  30 ]
                //          ]
                //      ]
                // ]
            ]
        ];
        $result = $elastic->search($params);
        $result = $this->getProductsIds($result);
        return $result;
    }
    
    public function getProductsIds($arr)
    {
        $result = [];
        foreach($arr['hits']['hits'] as $product)
        {
            $result[] = $product['_source']['product_id'];
        }
        return $result;
    }
    
    public function getSections($arr)
    {
        $result = [];
        foreach($arr['hits']['hits'] as $section)
        {
            $result[] = $section['_source'];
        }
        return $result;
    }
    
    //Entry point
    public function searchSections($str)
    {
        $result = [];
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
        $params = 
        [
            'index' => $config['INDEX_SECTIONS_NAME'],
            'body'  => 
            [
                'query' => 
                [
                    'match' => 
                    [
                        'SEARCHABLE_CONTENT' => $str
                    ]
                ]
            ]
        ];
        $result = $elastic->search($params);
        $result = $this->getSections($result);
        return $result;
    }

}