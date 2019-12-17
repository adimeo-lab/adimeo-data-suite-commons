<?php

namespace AdimeoDataSuite\Client;

use GuzzleHttp\Client;

class ElasticsearchClient
{

  private $elasticsearchServerUrl;

  /**
   * @var Client
   */
  private $client;

  private $stats = null;
  private $structure = null;

  public function __construct($elasticsearchServerUrl)
  {
    $this->elasticsearchServerUrl = $elasticsearchServerUrl;
    $this->client = new Client();
  }

  private function callAPI($uri, $options = [], $method = 'GET') {
    $options['headers']['Content-Type'] = 'application/json; charset=utf8';
    $r = $this->client->request($method, $this->elasticsearchServerUrl . $uri, $options);
    $body = (string)$r->getBody();
    return json_decode($body, true);
  }

  public function getStats() {
    if($this->stats == null) {
      $this->stats = $this->callAPI('/_stats');
    }
    return $this->stats;
  }

  public function getIndices() {
    return $this->getStats()['indices'];
  }

  public function getStructure() {
    if($this->structure == null) {
      $this->structure = $this->callAPI('/_all');
    }
    return $this->structure;
  }

  public function getMapping($indexName, $mappingName) {
    $mappings = $this->callAPI('/' . $indexName)[$indexName]['mappings'];
    if(isset($mappings[$mappingName])) {
      return $mappings[$mappingName];
    }
    return null;
  }

  public function search($indexName, $body, $type = null) {
    $uri = '/' . $indexName . ($type != null ? '/' . $type : '') . '/_search';
    $options = [
      'body' => json_encode($body)
    ];
    return $this->callAPI($uri, $options);
  }

  public function getServerInfo() {
    return $this->callAPI('/');
  }

  public function getClusterHealth() {
    return $this->callAPI('/_cluster/health');
  }

  public function createIndex($indexName, $settings) {
    $this->callAPI('/' . $indexName, [
      'body' => json_encode(['settings' => $settings])
    ], 'PUT');
  }

  public function updateIndexSettings($indexName, $settings) {
    $this->callAPI('/' . $indexName . '/_settings', [
      'body' => json_encode($settings)
    ], 'PUT');
  }

  public function closeIndex($indexName) {
    $this->callAPI('/' . $indexName . '/_close', [], 'POST');
  }

  public function openIndex($indexName) {
    $this->callAPI('/' . $indexName . '/_open', [], 'POST');
  }

  public function deleteIndex($indexName) {
    $this->callAPI('/' . $indexName, [], 'DELETE');
  }

  public function putMapping($indexName, $mappingName, $properties, $dynamicTemplates = null) {
    $params = [
      'properties' => $properties
    ];
    if($dynamicTemplates != null) {
      $params['dynamic_templates'] = $dynamicTemplates;
    }
    $this->callAPI('/' . $indexName . '/_mapping/' . $mappingName, [
      'body' => json_encode($params)
    ], 'PUT');
  }

  public function index($indexName, $mappingName, $document, $id = null) {

    return $this->callAPI('/' . $indexName . '/' . $mappingName . ($id != null ? '/' . $id : '/'), [
      'body' => json_encode($document)
    ], $id != null ? 'PUT' : 'POST');

  }

  public function delete($indexName, $mappingName, $id) {

    $this->callAPI('/' . $indexName . '/' . $mappingName . '/' . $id, [], 'DELETE');

  }

  public function flush() {
    $this->callAPI('/_all/_flush', [], 'POST');
  }

}