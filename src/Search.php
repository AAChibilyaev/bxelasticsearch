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
                                        "плитка,плиточка",
                                        "плитка,кафель"
                                    ]
                                ],
                                "ru_stopwords"=> 
                                [
                                    "type"=> "stop",
                                    "stopwords"=> "а,без,более,бы,был,была,были,было,быть,в,вам,вас,весь,во,вот,все,всего,всех,вы,где,да,даже,для,до,его,ее,если,есть,еще,же,за,здесь,и,из,или,им,их,к,как,ко,когда,кто,ли,либо,мне,может,мы,на,надо,наш,не,него,нее,нет,ни,них,но,ну,о,об,однако,он,она,они,оно,от,очень,по,под,при,с,со,так,также,такой,там,те,тем,то,того,тоже,той,только,том,ты,у,уже,хотя,чего,чей,чем,что,чтобы,чье,чья,эта,эти,это,я,a,an,and,are,as,at,be,but,by,for,if,in,into,is,it,no,not,of,on,or,such,that,the,their,then,there,these,they,this,to,was,will,with"
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
		'а' => 'a',    'б' => 'b',    'в' => 'v',    'г' => 'g',    'д' => 'd',
		'е' => 'e',    'ё' => 'e',    'ж' => 'zh',   'з' => 'z',    'и' => 'i',
		'й' => 'y',    'к' => 'k',    'л' => 'l',    'м' => 'm',    'н' => 'n',
		'о' => 'o',    'п' => 'p',    'р' => 'r',    'с' => 's',    'т' => 't',
		'у' => 'u',    'ф' => 'f',    'х' => 'h',    'ц' => 'c',    'ч' => 'ch',
		'ш' => 'sh',   'щ' => 'sch',  'ь' => '',     'ы' => 'y',    'ъ' => '',
		'э' => 'e',    'ю' => 'yu',   'я' => 'ya',
 
		'А' => 'A',    'Б' => 'B',    'В' => 'V',    'Г' => 'G',    'Д' => 'D',
		'Е' => 'E',    'Ё' => 'E',    'Ж' => 'Zh',   'З' => 'Z',    'И' => 'I',
		'Й' => 'Y',    'К' => 'K',    'Л' => 'L',    'М' => 'M',    'Н' => 'N',
		'О' => 'O',    'П' => 'P',    'Р' => 'R',    'С' => 'S',    'Т' => 'T',
		'У' => 'U',    'Ф' => 'F',    'Х' => 'H',    'Ц' => 'C',    'Ч' => 'Ch',
		'Ш' => 'Sh',   'Щ' => 'Sch',  'Ь' => '',     'Ы' => 'Y',    'Ъ' => '',
		'Э' => 'E',    'Ю' => 'Yu',   'Я' => 'Ya',
        );
    
        $value = strtr($value, $converter);
        return $value;
    }
   
    private function myTranslit($value)
    {
	    $converter = array(
		'а' => 'f',    'б' => ',',    'в' => 'd',    'г' => 'u',    'д' => 'l',
		'е' => 't',    'ё' => '`',    'ж' => ';',   'з' => 'p',    'и' => 'b',
		'й' => 'q',    'к' => 'r',    'л' => 'k',    'м' => 'v',    'н' => 'y',
		'о' => 'j',    'п' => 'g',    'р' => 'h',    'с' => 'c',    'т' => 'n',
		'у' => 'e',    'ф' => 'a',    'х' => '[',    'ц' => 'w',    'ч' => 'x',
		'ш' => 'i',   'щ' => 'o',  'ь' => 'm',     'ы' => 's',    'ъ' => ']',
		'э' => '',    'ю' => '.',   'я' => 'z',
 
		'А' => 'F',    'Б' => '<',    'В' => 'D',    'Г' => 'U',    'Д' => 'L',
		'Е' => 'T',    'Ё' => '~',    'Ж' => ':',   'З' => 'P',    'И' => 'B',
		'Й' => 'Q',    'К' => 'R',    'Л' => 'K',    'М' => 'V',    'Н' => 'Y',
		'О' => 'J',    'П' => 'G',    'Р' => 'H',    'С' => 'C',    'Т' => 'N',
		'У' => 'E',    'Ф' => 'A',    'Х' => '{',    'Ц' => 'W',    'Ч' => 'X',
		'Ш' => 'I',   'Щ' => 'O',  'Ь' => 'M',     'Ы' => 'S',    'Ъ' => '}',
		'Э' => '',    'Ю' => '>',   'Я' => 'Z',
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