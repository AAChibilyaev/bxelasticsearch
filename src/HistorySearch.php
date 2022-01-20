<?php
/*
 * Copyright (c) 2021, AAChibilyaev
 *
 * Chibilyaev Alexandr <info@aachibilyaev.com>
 *
 * AAChibilyaev LTD <https://aachibilyaev.com/>
 */

namespace aachibilyaev\bxelasticsearch;
class HistorySearch extends ElkClient
{
	public function __construct()
	{
		parent::__construct();
	}

    public function appendQuery($str = '')
    {
        if(!empty($str))
        {
            $config = ElkClient::getConfig();
            $elastic = ElkClient::client();
            $bulk = ['body' => []];
            $bulk['body'][] = ['create' => ['_index' => $config['INDEX_HISTORY_SEARCH_NAME']]];
            $bulk['body'][] = ['query'=>$str, 'user'=>json_encode($_SERVER), 'time'=>time() ];
            $elastic->bulk($bulk);
        }
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
		$elastic->indices()->delete(['index' => $config['INDEX_HISTORY_SEARCH_NAME']]);
	}
	
	public function createIndex(): void
	{
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$elastic->indices()->create(['index' => $config['INDEX_HISTORY_SEARCH_NAME'], 'body' => ['settings' => array('number_of_shards' => 1, 'number_of_replicas' => 0, 'index.mapping.total_fields.limit' => 600000, 'index.max_result_window' => 500000, 'analysis' => ['analyzer' => ['my_analyzer' => ['tokenizer' => 'my_tokenizer']], 'tokenizer' => ['my_tokenizer' => ['type' => 'ngram', 'min_gram' => 3, 'max_gram' => 3, 'token_chars' => ['letter', 'digit']]]]),]]);
		$mappingData = $this->elasticMapping();
		$elastic->indices()->putMapping(['index' => $config['INDEX_HISTORY_SEARCH_NAME'], 'body' => $mappingData]);
	}
    
    public function elasticMapping(): array
	{
		$result = [];
        $result['properties']['query']['type'] = 'text';
        $result['properties']['user']['type'] = 'text';
        $result['properties']['time']['type'] = 'integer';
		return $result;
	}

	public function getAll()
	{
		$result = [];
		$config = ElkClient::getConfig();
		$elastic = ElkClient::client();
		$params = 
        [
            'index' => $config['INDEX_HISTORY_SEARCH_NAME'],
			'size'=>6000
			// '_source' => false
        ];
        
        $response = $elastic->search($params);
		if($response['hits']['total']['value']>0)
		{
			foreach($response['hits']['hits'] as $item)
				$result[] =  $item['_source']['query'];
		}
		
		return $result;

	}

}
