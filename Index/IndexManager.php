<?php

namespace AdimeoDataSuite\Index;

use AdimeoDataSuite\Model\Autopromote;
use AdimeoDataSuite\Model\PersistentObject;
use AdimeoDataSuite\Model\SecurityContext;
use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use Elasticsearch\Common\Exceptions\NoNodesAvailableException;

class IndexManager
{

    const APP_INDEX_NAME = '.adimeo_data_suite';
    const APP_RECO_INDEX_NAME = '.adimeo_data_suite_reco';

    /**
     * @var Client
     */
    private $client;

    public function __construct($elasticsearchServerUrl) {
        $clientBuilder = new ClientBuilder();
        if(!defined('JSON_PRESERVE_ZERO_FRACTION')){
            $clientBuilder->allowBadJSONSerialization();
        }
        $clientBuilder->setHosts(array($elasticsearchServerUrl));
        $this->client = $clientBuilder->build();
    }

    /**
     * @return Client
     */
    public function getClient() {
        return $this->client;
    }

    public function getServerInfo() {
        return array(
            'server_info' => $this->client->info(),
            'health' => $this->client->cluster()->health(),
            'stats' => $this->client->cluster()->stats(),
        );
    }

    public function getIndicesList(SecurityContext $context = NULL) {
        $mappings = $this->client->indices()->getMapping();
        $settings = $this->client->indices()->getSettings();
        $indices = $this->client->indices()->stats()['indices'];
        foreach($indices as $index => $stats) {
            if(isset($settings[$index])) {
                $indices[$index]['settings'] = $settings[$index]['settings'];
            }
            if(isset($mappings[$index])) {
                $indices[$index]['mappings'] = $mappings[$index]['mappings'];
            }
        }
        ksort($indices);
        if($context != NULL && !$context->isAdmin()) {
            foreach($indices as $k => $data) {
                if(!in_array($k, $context->getRestrictions()['indexes']))
                    unset($indices[$k]);
            }
        }
        return $indices;
    }

    function getIndicesInfo(SecurityContext $context = NULL)
    {
        $info = array();
        $stats = $this->client->indices()->stats();
        foreach ($stats['indices'] as $index_name => $stat) {
            $info[$index_name] = array(
                'count' => $stat['total']['docs']['count'] - $stat['total']['docs']['deleted'],
                'size' => round($stat['total']['store']['size_in_bytes'] / 1024 / 1024, 2) . ' MB',
            );
            $mappings = $this->client->indices()->getMapping(array('index' => $index_name));
            foreach ($mappings[$index_name]['mappings'] as $mapping => $properties) {
                $info[$index_name]['mappings'][] = array(
                    'name' => $mapping,
                    'field_count' => count($properties['properties']),
                );
            }
        }
        ksort($info);
        if($context != NULL && !$context->isAdmin()) {
            foreach($info as $k => $data) {
                if(!in_array($k, $context->getRestrictions()['indexes']))
                    unset($info[$k]);
            }
        }
        unset($stats);
        return $info;
    }

    public function getIndex($indexName) {
        try {
            return $this->client->indices()->getSettings(array('index' => $indexName));
        }
        catch(Missing404Exception $ex) {
            return null;
        }
    }

    public function createIndex($indexName, $settings) {
        if (isset($settings['creation_date']))
            unset($settings['creation_date']);
        if (isset($settings['version']))
            unset($settings['version']);
        if (isset($settings['uuid']))
            unset($settings['uuid']);
        if (isset($settings['provided_name']))
            unset($settings['provided_name']);
        $params = array(
            'index' => $indexName,
        );
        $settings['analysis']['analyzer']['transliterator'] = array(
            'filter' => array('standard', 'asciifolding', 'lowercase'),
            'tokenizer' => 'keyword'
        );
        if (count($settings) > 0) {
            $params['body'] = array(
                'settings' => $settings,
            );
        }
        return $this->client->indices()->create($params);
    }

    function updateIndex($indexName, $settings)
    {
        $this->client->indices()->close(array(
            'index' => $indexName
        ));
        if (isset($settings['creation_date']))
            unset($settings['creation_date']);
        if (isset($settings['version']))
            unset($settings['version']);
        if (isset($settings['uuid']))
            unset($settings['uuid']);
        if (isset($settings['number_of_shards']))
            unset($settings['number_of_shards']);
        if (isset($settings['number_of_replicas']))
            unset($settings['number_of_replicas']);
        //if (isset($settings['analysis']))
        //  unset($settings['analysis']);
        if (isset($settings['provided_name']))
            unset($settings['provided_name']);
        if (count($settings) > 0) {
            try {
                $this->client->indices()->putSettings(array(
                    'index' => $indexName,
                    'body' => array(
                        'settings' => $settings,
                    ),
                ));
            }
            catch(\Exception $ex) {

            }
        }
        $this->client->indices()->open(array(
            'index' => $indexName
        ));
    }

    public function deleteIndex($indexName) {
        $params = array(
            'index' => $indexName
        );
        return $this->client->indices()->delete($params);
    }

    /**
     *
     * @param string $indexName
     * @return string[]
     */
    function getAnalyzers($indexName)
    {
        $analyzers = array('standard', 'simple', 'whitespace', 'stop', 'keyword', 'pattern', 'language', 'snowball');
        $settings = $this->client->indices()->getSettings(array(
            'index' => $indexName,
        ));
        if (isset($settings[$indexName]['settings']['index']['analysis']['analyzer'])) {
            foreach ($settings[$indexName]['settings']['index']['analysis']['analyzer'] as $analyzer => $definition) {
                $analyzers[] = $analyzer;
            }
        }
        unset($settings);
        return $analyzers;
    }

    /**
     *
     * @return string[]
     */
    function getFieldTypes()
    {
        $types = array('integer', 'long', 'float', 'double', 'boolean', 'date', 'ip', 'geo_point');
        if($this->getServerMajorVersionNumber() >= 5){
            $types = array_merge($types, array('text', 'keyword'));
        }
        else{
            $types = array_merge($types, array('string'));
        }
        asort($types);
        return $types;
    }

    /**
     *
     * @return string[]
     */
    function getDateFormats()
    {
        return array('basic_date', 'basic_date_time', 'basic_date_time_no_millis', 'basic_ordinal_date', 'basic_ordinal_date_time', 'basic_ordinal_date_time_no_millis', 'basic_time', 'basic_time_no_millis', 'basic_t_time', 'basic_t_time_no_millis', 'basic_week_date', 'basic_week_date_time', 'basic_week_date_time_no_millis', 'date', 'date_hour', 'date_hour_minute', 'date_hour_minute_second', 'date_hour_minute_second_fraction', 'date_hour_minute_second_millis', 'date_optional_time', 'date_time', 'date_time_no_millis', 'hour', 'hour_minute', 'hour_minute_second', 'hour_minute_second_fraction', 'hour_minute_second_millis', 'ordinal_date', 'ordinal_date_time', 'ordinal_date_time_no_millis', 'time', 'time_no_millis', 't_time', 't_time_no_millis', 'week_date', 'week_date_time', 'weekDateTimeNoMillis', 'week_year', 'weekyearWeek', 'weekyearWeekDay', 'year', 'year_month', 'year_month_day');
    }

    function getServerMajorVersionNumber(){
        $info = $this->getServerInfo()['server_info'];
        return (int)explode('.', $info['version']['number'])[0];
    }

    public function putMapping($indexName, $mappingName, $mapping, $dynamicTemplates = NULL, $wipeData = false) {
        if ($wipeData) {
            $this->deleteByQuery($indexName, $mappingName, array(
                'query' => array(
                    'match_all' => array('boost' => 1)
                )
            ));
        }

        $body = array(
            'properties' => $mapping
        );
        if($dynamicTemplates != NULL) {
            $body['dynamic_templates'] = $dynamicTemplates;
        }
        $this->client->indices()->putMapping(array(
            'index' => $indexName,
            'type' => $mappingName,
            'body' => $body
        ));
    }

    function getMapping($indexName, $mappingName)
    {
        try {
            $mapping = $this->client->indices()->getMapping(array(
                'index' => $indexName,
                'type' => $mappingName,
            ));
            if (isset($mapping[$indexName]['mappings'][$mappingName])) {
                return $mapping[$indexName]['mappings'][$mappingName];
            } else
                return null;
        } catch (\Exception $ex) {
            return null;
        }
    }

    public function initStore($numberOfShards = 1, $numberOfReplicas = 1) {
        $indices = array_keys($this->getIndicesList());
        if(!in_array(static::APP_INDEX_NAME, $indices)) {
            $json = json_decode(file_get_contents(__DIR__ . '/../Resources/store_structure.json'), TRUE);
            $indexSettings = $json['index'];
            $indexSettings['number_of_shards'] = $numberOfShards;
            $indexSettings['number_of_replicas'] = $numberOfReplicas;
            $this->createIndex(static::APP_INDEX_NAME, $indexSettings);
        }
        $mapping = $this->getMapping(static::APP_INDEX_NAME, 'store_item');
        if($mapping == null) {
            $json = json_decode(file_get_contents(__DIR__ . '/../Resources/store_structure.json'), TRUE);
            $this->putMapping(static::APP_INDEX_NAME, 'store_item', $json['mapping']);
        }
    }

    public function search($indexName, $query, $from = 0, $size = 20, $type = null) {
        $this->sanitizeGlobalAgg($query);
        $params = array(
            'index' => $indexName,
            'body' =>$query
        );
        if($type != null) {
            $params['type'] = $type;
        }
        $params['body']['from'] = $from;
        $params['body']['size'] = $size;
        return $this->client->search($params);
    }

    private function sanitizeGlobalAgg(&$array)
    { //Bug fix form empty queries in global aggregations
        if ($array != null) {
            foreach ($array as $k => $v) {
                if ($k == 'global' && empty($v) || $k == 'reverse_nested' && empty($v))
                    $array[$k] = new \stdClass();
                elseif (is_array($v))
                    $this->sanitizeGlobalAgg($array[$k]);
            }
        }
    }

    public function persistObject(PersistentObject $o) {
        $created = new \DateTime();
        $updated = new \DateTime();
        $o->setUpdated($updated);
        if($o->getId() == null) {
            $o->setCreated($created);
        }
        $params = array(
            'index' => static::APP_INDEX_NAME,
            'type' => 'store_item',
            'body' => array(
                'name' => $o->getName(),
                'type' => $o->getType(),
                'created_by' => $o->getCreatedBy(),
                'tags' => $o->getTags(),
                'data' => $o->serialize(),
                'updated' => $updated->format('Y-m-d\TH:i:s')
            )
        );
        if($o->getId() != null) {
            $params['id'] = $o->getId();
            if($o->getCreated() == null) {
                $o->setCreated(new \DateTime());
            }
            $params['body']['created'] = $o->getCreated()->format('Y-m-d\TH:i:s');
        }
        else {
            $params['body']['created'] = $created->format('Y-m-d\TH:i:s');
        }
        $r = $this->client->index($params);
        if(isset($r['_id'])){
            $o->setId($r['_id']);
        }
        $this->client->indices()->flush();
        return $o;
    }

    public function deleteObject($id) {
        $this->client->delete(array(
            'index' => static::APP_INDEX_NAME,
            'type' => 'store_item',
            'id' => $id
        ));
        $this->client->indices()->flush();
    }

    /**
     * @param string $type
     * @param string $id
     * @return PersistentObject
     */
    public function findObject($type, $id) {
        $query = array(
            'query' => array(
                'bool' => array(
                    'must' => array(
                        array(
                            'term' => array(
                                'type' => $type
                            )
                        ),
                        array(
                            'ids' => array(
                                'values' => array($id)
                            )
                        )
                    )
                )
            )
        );
        $r = $this->search(static::APP_INDEX_NAME, $query);
        if(isset($r['hits']['hits'][0])) {
            $object = PersistentObject::unserialize($r['hits']['hits'][0]['_source']['data']);
            if($object instanceof PersistentObject) {
                $object->setId($r['hits']['hits'][0]['_id']);
                return $object;
            }
            else {
                return NULL;
            }
        }
        else {
            return null;
        }
    }

    /**
     * @param string $type
     * @param SecurityContext|NULL $context
     * @param int $from
     * @param int $size
     * @param string $order
     * @param array $criterias
     * @return PersistentObject[]
     */
    public function listObjects($type, SecurityContext $context = NULL, $from = 0, $size = 10000, $order = 'asc', $criterias = []) {
        $query = array(
            'query' => array(
                'bool' => array(
                    'should' => array(
                        array(
                            'bool' => array(
                                'must' => [
                                    array(
                                        'term' => array(
                                            'type' => $type
                                        )
                                    )
                                ]
                            )
                        )
                    )
                )
            ),
            'size' => $size,
            'from' => $from,
            'sort' => array(
                'name.raw' => $order
            )
        );
        foreach($criterias as $criteriaName => $criteriaValue) {
            $query['query']['bool']['should'][0]['bool']['must'][] = array(
                'term' => array(
                    $criteriaName => $criteriaValue
                )
            );
        }
        if($context != null && !$context->isAdmin()) {
            $restricted = array(
                'datasource' => 'datasources',
                'matching_list' => 'matchingLists'
            );
            foreach($restricted as $restrictionType => $restriction) {
                if ($type == $restrictionType) {
                    $query['query']['bool']['should'][0]['bool']['must'][] = array(
                        'ids' => array(
                            'values' => $context->getRestrictions()[$restriction]
                        )
                    );
                }
            }
            if($type == 'processor') {
                $procQuery = array(
                    'bool' => array(
                        'must' => array(
                            array(
                                'bool' => array(
                                    'should' => []
                                )
                            ),
                            array(
                                'bool' => array(
                                    'should' => []
                                )
                            )
                        )
                    )
                );
                foreach($context->getRestrictions()['datasources'] as $procDs) {
                    $procQuery['bool']['must'][0]['bool']['should'][] = array(
                        'term' => array(
                            'tags' => 'datasource_id=' . $procDs
                        )
                    );
                }
                foreach($context->getRestrictions()['indexes'] as $procIndexes) {
                    $procQuery['bool']['must'][1]['bool']['should'][] = array(
                        'term' => array(
                            'tags' => 'index_name=' . $procIndexes
                        )
                    );
                }
                $query['query']['bool']['should'][0]['bool']['must'][] = $procQuery;
            }
            if($type == 'search_page') {
                $spQuery = array(
                    'bool' => array(
                        'must' => array(
                            array(
                                'bool' => array(
                                    'should' => []
                                )
                            )
                        )
                    )
                );
                foreach($context->getRestrictions()['indexes'] as $spIndexes) {
                    $spQuery['bool']['must'][0]['bool']['should'][] = array(
                        'term' => array(
                            'tags' => 'index_name=' . $spIndexes
                        )
                    );
                }
                $query['query']['bool']['should'][0]['bool']['must'][] = $spQuery;
            }
            if($type == 'boost_query') {
                $bqQuery = array(
                    'bool' => array(
                        'must' => array(
                            array(
                                'bool' => array(
                                    'should' => []
                                )
                            )
                        )
                    )
                );
                foreach($context->getRestrictions()['indexes'] as $bqIndexes) {
                    $bqQuery['bool']['must'][0]['bool']['should'][] = array(
                        'term' => array(
                            'tags' => 'index_name=' . $bqIndexes
                        )
                    );
                }
                $query['query']['bool']['should'][0]['bool']['must'][] = $bqQuery;
            }
            if($type == 'saved_query') {
                $sqQuery = array(
                    'bool' => array(
                        'must' => array(
                            array(
                                'bool' => array(
                                    'should' => []
                                )
                            )
                        )
                    )
                );
                foreach($context->getRestrictions()['indexes'] as $sqIndexes) {
                    $sqQuery['bool']['must'][0]['bool']['should'][] = array(
                        'term' => array(
                            'tags' => 'index_name=' . $sqIndexes
                        )
                    );
                }
                $query['query']['bool']['should'][0]['bool']['must'][] = $sqQuery;
            }
            $query['query']['bool']['should'][] = array(
                'bool' => array(
                    'must' => array(
                        array(
                            'term' => array(
                                'created_by' => $context->getUserUid()
                            )
                        ),
                        array(
                            'term' => array(
                                'type' => $type
                            )
                        )
                    )
                )
            );
        }
        $r = $this->search(static::APP_INDEX_NAME, $query, $from, $size);
        $objects = [];
        foreach($r['hits']['hits'] as $hit) {
            $object = PersistentObject::unserialize($hit['_source']['data']);
            if($object instanceof PersistentObject) {
                $object->setId($hit['_id']);
                $objects[] = $object;
            }
        }
        return $objects;
    }

    public function deleteByQuery($indexName, $mappingName, $query)
    {
        if($this->getServerMajorVersionNumber() >= 5) {
            $this->client->deleteByQuery(array(
                'index' => $indexName,
                'type' => $mappingName,
                'body' => $query
            ));
        }
        else{
            //Delete by query is not available on ES 2.x clusters so let's do it on our own
            $this->scroll($query, $indexName, $mappingName, function($items){
                $this->bulkDelete($items);
            },500);
        }
    }

    public function scroll($queryBody, $index, $mapping, $callback, $size = 10, $context = null)
    {
        $params = array(
            'index' => $index,
            'body' => $queryBody,
            'scroll' => '10ms',
            'size' => $size
        );
        if($mapping != null) {
            $params['type'] = $mapping;
        }
        $r = $this->client->search($params);
        if (isset($r['_scroll_id'])) {
            while (count($r['hits']['hits']) > 0) {
                $callback($r['hits']['hits'], $context);
                $scrollId = $r['_scroll_id'];
                $r = $this->client->scroll(array(
                    'scroll_id' => $scrollId,
                    'scroll' => '1m'
                ));
            }
        }
    }

    public function bulkDelete($items)
    {
        $bulkString = '';
        foreach ($items as $item) {
            $data = array('delete' => array('_index' => $item['_index'], '_type' => $item['_type'], '_id' => $item['_id']));
            $bulkString .= json_encode($data) . "\n";
        }
        if (count($items) > 0) {
            $params['index'] = $items[0]['_index'];
            $params['type'] = $items[0]['_type'];
            $params['body'] = $bulkString;
            $this->client->bulk($params);
        }
    }

    public function getAutopromoteIndexName($fromIndex) {
        return '.ads_autopromote_' . str_replace('.', '', $fromIndex);
    }

    public function getAutopromoteAnalyzer($fromIndex) {
        $mapping = $this->getMapping($this->getAutopromoteIndexName($fromIndex), 'autopromote');
        return $mapping['properties']['keywords']['analyzer'];
    }

    public function createAutopromoteIndex($fromIndex, $analyzer) {
        $index = $this->getIndex($fromIndex);
        $indexName = $this->getAutopromoteIndexName($fromIndex);
        $this->createIndex('.ads_autopromote_' . str_replace('.', '', $fromIndex), array(
            'analysis' => $index[$fromIndex]['settings']['index']['analysis']
        ));
        $json = json_decode(file_get_contents(__DIR__ . '/../Resources/autopromote_structure.json'), TRUE);
        $json['mapping']['keywords']['analyzer'] = $analyzer;
        $this->putMapping($indexName, 'autopromote', $json['mapping']);
    }

    public function saveAutopromote(Autopromote $autopromote) {
        $doc = array(
            'name' => $autopromote->getName(),
            'keywords' => $autopromote->getKeywords(),
            'data' => serialize($autopromote)
        );
        $params = array(
            'index' => $this->getAutopromoteIndexName($autopromote->getIndex()),
            'type' => 'autopromote',
            'body' => $doc
        );
        if($autopromote->getId() != NULL) {
            $params['id'] = $autopromote->getId();
        }
        $r = $this->client->index($params);
        if(isset($r['_id'])){
            $autopromote->setId($r['_id']);
        }
        $this->client->indices()->flush();
    }

    public function listAutopromotes(SecurityContext $securityContext = null) {
        $params = array(
            'from' => 0,
            'size' => 10000,
            'body' => array(
                'query' => array(
                    'match_all' => array(
                        'boost' => 1
                    )
                )
            )
        );
        if($securityContext == null || $securityContext->isAdmin()) {
            $params['index'] = '.ads_autopromote_*';
        }
        else {
            $indexRestrictions = $securityContext->getRestrictions()['indexes'];
            if(empty($indexRestrictions)) {
                return [];
            }
            else {
                $restrictedIndexes = [];
                foreach($indexRestrictions as $ir) {
                    if($this->getIndex($this->getAutopromoteIndexName($ir)) != null)
                        $restrictedIndexes[] = $this->getAutopromoteIndexName($ir);
                }
                if(empty($restrictedIndexes))
                    return [];
                $params['index'] = implode(',', $restrictedIndexes);
            }
        }
        $r = $this->client->search($params);
        $autopromotes = [];
        foreach($r['hits']['hits'] as $hit) {
            $autopromote = unserialize($hit['_source']['data']);
            $autopromote->setId($hit['_id']);
            $autopromotes[] = $autopromote;
        }
        return $autopromotes;
    }

    public function getAutopromote($id, $index) {
        $params = array(
            'from' => 0,
            'size' => 1,
            'body' => array(
                'query' => array(
                    'ids' => array(
                        'values' => [$id]
                    )
                )
            )
        );
        $params['index'] = $this->getAutopromoteIndexName($index);
        $r = $this->client->search($params);
        if(isset($r['hits']['hits'][0])) {
            $autopromote = unserialize($r['hits']['hits'][0]['_source']['data']);
            $autopromote->setId($r['hits']['hits'][0]['_id']);
            return $autopromote;
        }
        return NULL;
    }

    public function deleteAutopromote($id, $index) {
        $this->client->delete(array(
            'index' => $this->getAutopromoteIndexName($index),
            'type' => 'autopromote',
            'id' => $id
        ));
        $this->client->indices()->flush();
    }

    public function bulkIndex($items)
    {
        $bulkString = '';
        foreach ($items as $item) {
            $data = array('index' => array('_index' => $item['indexName'], '_type' => $item['mappingName']));
            if (isset($item['body']['_id'])) {
                $data['index']['_id'] = $item['body']['_id'];
                unset($item['body']['_id']);
            }
            $bulkString .= json_encode($data) . "\n";
            $bulkString .= json_encode($item['body']) . "\n";
        }
        if (count($items) > 0) {
            $params['index'] = $items[0]['indexName'];
            $params['type'] = $items[0]['mappingName'];
            $params['body'] = $bulkString;

            $tries = 0;
            $retry = true;
            while ($tries == 0 || $retry) {
                try {
                    $this->client->bulk($params);
                    $retry = false;
                } catch (NoNodesAvailableException $ex) {
                    print get_class($this) . ' >> NoNodesAvailableException has been caught (' . $ex->getMessage() . ')' . PHP_EOL;
                    if ($tries > 20) {
                        $retry = false;
                        print get_class($this) . ' >> This is over, I choose to die.' . PHP_EOL;
                        return; //Kill the datasource
                    } else {
                        print get_class($this) . ' >> Retrying in 1 second...' . PHP_EOL;
                        sleep(1); //Sleep for 1 second
                    }
                } finally {
                    $tries++;
                }
            }
        }
    }

    public function indexDocument($indexName, $mappingName, $document, $flush = true)
    {
        $id = null;
        if (isset($document['_id'])) {
            $id = $document['_id'];
            unset($document['_id']);
        }
        $params = array(
            'index' => $indexName,
            'type' => $mappingName,
            'body' => $document,
        );
        if ($id != null) {
            $params['id'] = $id;
        }
        $tries = 0;
        $retry = true;
        while ($tries == 0 || $retry) {
            try {
                $r = $this->client->index($params);
                if ($flush) {
                    $this->client->indices()->flush();
                }
                $retry = false;
            } catch (NoNodesAvailableException $ex) {
                print get_class($this) . ' >> NoNodesAvailableException has been caught (' . $ex->getMessage() . ')' . PHP_EOL;
                if ($tries > 20) {
                    $retry = false;
                    print get_class($this) . ' >> This is over, I choose to die.' . PHP_EOL;
                    return null; //Kill the datasource
                } else {
                    print get_class($this) . ' >> Retrying in 1 second...' . PHP_EOL;
                    sleep(1); //Sleep for 1 second
                }
            } finally {
                $tries++;
            }
        }
        unset($params);
        return isset($r) ? $r : null;
    }

    public function flush() {
        $this->client->indices()->flush();
    }

    /**
     * Get Elastic information
     *
     * @param SecurityContext $securityContext
     * @param bool $checkACL
     * @return array
     */
    public function getElasticInfo(SecurityContext $securityContext, $checkACL = true)
    {
        $info = array();
        $stats = $this->getClient()->indices()->stats();
        $allowed_indexes = ($checkACL) ? $securityContext->getRestrictions()['indexes'] : [];
        foreach ($stats['indices'] as $index_name => $stat) {
            if (!$checkACL || $securityContext->isAdmin() || in_array($index_name, $allowed_indexes)) {
                $info[$index_name] = array(
                    'count' => $stat['total']['docs']['count'] - $stat['total']['docs']['deleted'],
                    'size' => round($stat['total']['store']['size_in_bytes'] / 1024 / 1024, 2) . ' MB',
                );
                $mappings = $this->getClient()->indices()->getMapping(array('index' => $index_name));
                foreach ($mappings[$index_name]['mappings'] as $mapping => $properties) {
                    $info[$index_name]['mappings'][] = array(
                        'name' => $mapping,
                        'field_count' => count($properties['properties']),
                    );
                }
                unset($mappings);
            }
        }
        unset($stats);
        return $info;
    }

    /**
     * @param $id
     * @param $index
     * @param $type
     */
    public function deleteObjectInIndex($id, $index, $type)
    {
        $this->client->delete(array(
            'index' => $index,
            'type' => $type,
            'id' => $id
        ));
        $this->client->indices()->flush();
    }
}
