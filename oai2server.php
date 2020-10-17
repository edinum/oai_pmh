<?php
require_once('oai2exception.php');
require_once('oai2xml.php');

/**
 * This is an implementation of OAI Data Provider version 2.0.
 * @see http://www.openarchives.org/OAI/2.0/openarchivesprotocol.htm
 */
class OAI2Server {

    public $errors = array();
    private $args = array();
    private $verb = '';
    private $token_prefix = '/tmp/oai_pmh-';
    private $token_valid = 0;

    function __construct($uri, $args, $identifyResponse, $maxItems, $callbacks) {

        $this->uri = $uri;
        $this->maxItems = $maxItems;

        if (!isset($args['verb']) || empty($args['verb'])) {
            $this->errors[] = new OAI2Exception('badVerb');
        } else {
            $verbs = array('Identify', 'ListMetadataFormats', 'ListSets', 'ListIdentifiers', 'ListRecords', 'GetRecord');
            if (in_array($args['verb'], $verbs)) {

                $this->verb = $args['verb'];

                unset($args['verb']);

                $this->args = $args;

                $this->identifyResponse = $identifyResponse;

                $this->listMetadataFormatsCallback = $callbacks['ListMetadataFormats'];
                $this->listSetsCallback = $callbacks['ListSets'];
                $this->listRecordsCallback = $callbacks['ListRecords'];
                $this->getRecordCallback = $callbacks['GetRecord'];

                $this->response = new OAI2XMLResponse($this->uri, $this->verb, $this->args);

                call_user_func(array($this, $this->verb));

            } else {
                $this->errors[] = new OAI2Exception('badVerb');
            }
        }

    }

    public function response() {
        if (empty($this->errors)) {
            return $this->response->doc;
        } else {
            $errorResponse = new OAI2XMLResponse($this->uri, $this->verb, $this->args);
            $oai_node = $errorResponse->doc->documentElement;
            foreach($this->errors as $e) {
                $node = $errorResponse->addChild($oai_node,"error",$e->getMessage());
                $node->setAttribute("code",$e->getOAI2Code());
            }
            return $errorResponse->doc;
        }
    }

    public function Identify() {

        if (count($this->args) > 0) {
            foreach($this->args as $key => $val) {
                $this->errors[] = new OAI2Exception('badArgument');
            }
        } else {
            foreach($this->identifyResponse as $key => $val) {
                $this->response->addToVerbNode($key, $val);
            }
        }
    }

    public function ListMetadataFormats() {

        foreach ($this->args as $argument => $value) {
            if ($argument != 'identifier') {
                $this->errors[] = new OAI2Exception('badArgument');
            }
        }
        if (isset($this->args['identifier'])) {
            $identifier = $this->args['identifier'];
        } else {
            $identifier = '';
        }
        if (empty($this->errors)) {
            try {
                if ($formats = call_user_func($this->listMetadataFormatsCallback, $identifier)) {
                    foreach($formats as $key => $val) {
                        $cmf = $this->response->addToVerbNode("metadataFormat");
                        $this->response->addChild($cmf,'metadataPrefix',$key);
                        $this->response->addChild($cmf,'schema',$val['schema']);
                        $this->response->addChild($cmf,'metadataNamespace',$val['metadataNamespace']);
                    }
                } else {
                    $this->errors[] = new OAI2Exception('noMetadataFormats');
                }
            } catch (OAI2Exception $e) {
                $this->errors[] = $e;
            }
        }
    }

    public function ListSets() {

        if (isset($this->args['resumptionToken'])) {
            if (count($this->args) > 1) {
                $this->errors[] = new OAI2Exception('badArgument');
            } else {
                $resumptionToken = intval($this->args['resumptionToken']);
                if ($resumptionToken != $this->args['resumptionToken']) {
                    $this->errors[] = new OAI2Exception('badResumptionToken');
                }
            }
        } else {
            $resumptionToken = 0;
        }
        if (empty($this->errors)) {
            $count = call_user_func($this->listSetsCallback, true);

            if ($sets = call_user_func($this->listSetsCallback, false, $this->maxItems, $resumptionToken)) {

                foreach($sets as $set) {

                    $setNode = $this->response->addToVerbNode("set");

                    foreach($set as $key => $val) {
                        if($key=='setDescription') {
                            $desNode = $this->response->addChild($setNode,$key);
                            $des = $this->response->doc->createDocumentFragment();
                            $des->appendXML($val);
                            $desNode->appendChild($des);
                        } else {
                            $this->response->addChild($setNode,$key,$val);
                        }
                    }
                }

                // Will we need a new ResumptionToken?
                if ($count - $resumptionToken > $this->maxItems) {
                    $restoken +=  $this->maxItems;
                } elseif (isset($args['resumptionToken'])) {
                    // Last delivery, return empty ResumptionToken
                    $restoken = null;
                    $expirationDatetime = null;
                }

                if (isset($restoken)) {
                    $this->response->createResumptionToken($restoken, false, $count, $restoken);
                }

            } else {
                $this->errors[] = new OAI2Exception('noSetHierarchy');
            }
        }
    }

    public function GetRecord() {

        if (!isset($this->args['metadataPrefix'])) {
            $this->errors[] = new OAI2Exception('badArgument');
        } else {
            $metadataFormats = call_user_func($this->listMetadataFormatsCallback);
            if (!isset($metadataFormats[$this->args['metadataPrefix']])) {
                $this->errors[] = new OAI2Exception('cannotDisseminateFormat');
            }
        }
        if (!isset($this->args['identifier'])) {
            $this->errors[] = new OAI2Exception('badArgument');
        }

        if (empty($this->errors)) {
            try {
                if ($record = call_user_func($this->getRecordCallback, $this->args['identifier'], $this->args['metadataPrefix'])) {

                    $identifier = $record['identifier'];

                    $datestamp = $this->formatDatestamp($record['datestamp']);

                    $set = $record['set'];

                    $status_deleted = (isset($record['deleted']) && ($record['deleted'] == 'true') &&
                                       (($this->identifyResponse['deletedRecord'] == 'transient') ||
                                        ($this->identifyResponse['deletedRecord'] == 'persistent')));

                    $cur_record = $this->response->addToVerbNode('record');
                    $cur_header = $this->response->createHeader($identifier, $datestamp, $set, $cur_record);
                    if ($status_deleted) {
                        $cur_header->setAttribute("status","deleted");
                    } else {
                        $this->add_metadata($cur_record, $record);
                    }
                } else {
                    $this->errors[] = new OAI2Exception('idDoesNotExist');
                }
            } catch (OAI2Exception $e) {
                $this->errors[] = $e;
            }
        }
    }

    public function ListIdentifiers() {
        $this->ListRecords();
    }

    public function ListRecords() {

        $maxItems = $this->maxItems;
        $deliveredRecords = 0;
        $metadataPrefix = $this->args['metadataPrefix'];
        $from = isset($this->args['from']) ? $this->args['from'] : '';
        $until = isset($this->args['until']) ? $this->args['until'] : '';
        $set = isset($this->args['set']) ? $this->args['set'] : '';

        if (isset($this->args['resumptionToken'])) {
            if (count($this->args) > 1) {
                $this->errors[] = new OAI2Exception('badArgument');
            } else {
                if ($readings = $this->readResumptionToken($this->args['resumptionToken'])) {
                    list($deliveredRecords, $metadataPrefix, $from, $until, $set) = $readings;
                } else {
                    $this->errors[] = new OAI2Exception('badResumptionToken');
                }
            }
        } else {
            if (!isset($this->args['metadataPrefix'])) {
                $this->errors[] = new OAI2Exception('badArgument');
            } else {
                $metadataFormats = call_user_func($this->listMetadataFormatsCallback);
                if (!isset($metadataFormats[$this->args['metadataPrefix']])) {
                    $this->errors[] = new OAI2Exception('cannotDisseminateFormat');
                }
            }
            if (isset($this->args['from'])) {
                if(!$this->checkDateFormat($this->args['from'])) {
                    $this->errors[] = new OAI2Exception('badArgument');
                }
            }
            if (isset($this->args['until'])) {
                if(!$this->checkDateFormat($this->args['until'])) {
                    $this->errors[] = new OAI2Exception('badArgument');
                }
            }
        }

        if (empty($this->errors)) {
            try {
                # ListRecords or ListIdentifiers
                $list_records = $this->verb == 'ListRecords';

                $records_count = call_user_func($this->listRecordsCallback, $metadataPrefix, $from, $until, $set, true, $list_records);

                # TODO: must send $this->verb, to only get identifier if ListIdentifiers
                $records = call_user_func($this->listRecordsCallback, $metadataPrefix, $from, $until, $set, false, $list_records, $deliveredRecords, $maxItems);

                foreach ($records as $record) {

                    $identifier = $record['identifier'];
                    $datestamp = $this->formatDatestamp($record['datestamp']);
                    $setspec = $record['set'];

                    $status_deleted = (isset($record['deleted']) && ($record['deleted'] === true) &&
                                        (($this->identifyResponse['deletedRecord'] == 'transient') ||
                                         ($this->identifyResponse['deletedRecord'] == 'persistent')));

                    if($list_records) {
                        $cur_record = $this->response->addToVerbNode('record');
                        $cur_header = $this->response->createHeader($identifier, $datestamp,$setspec,$cur_record);
                        if (!$status_deleted) {
                            $this->add_metadata($cur_record, $record);
                        }	
                    } else { // for ListIdentifiers, only identifiers will be returned.
                        $cur_header = $this->response->createHeader($identifier, $datestamp,$setspec);
                    }
                    if ($status_deleted) {
                        $cur_header->setAttribute("status","deleted");
                    }
                }

                // Will we need a new ResumptionToken?
                if ($records_count - $deliveredRecords > $maxItems) {

                    $deliveredRecords +=  $maxItems;
                    $restoken = $this->createResumptionToken($deliveredRecords, $metadataPrefix, $from, $until, $set);

                    $expirationDatetime = $this->token_valid ? gmstrftime('%Y-%m-%dT%TZ', time()+$this->token_valid) : '';

                } elseif (isset($args['resumptionToken'])) {
                    // Last delivery, return empty ResumptionToken
                    $restoken = null;
                    $expirationDatetime = null;
                }

                if (isset($restoken)) {
                    $this->response->createResumptionToken($restoken,$expirationDatetime,$records_count,$deliveredRecords);
                }

            } catch (OAI2Exception $e) {
                $this->errors[] = $e;
            }
        }
    }

    private function add_metadata($cur_record, $record) {

        $meta_node =  $this->response->addChild($cur_record ,"metadata");

        $schema_node = $this->response->addChild($meta_node, $record['metadata']['container_name']);
        foreach ($record['metadata']['container_attributes'] as $name => $value) {
            $schema_node->setAttribute($name, $value);
        }
        foreach ($record['metadata']['fields'] as $name => $values) {
            # If value is a string treat it as single value
            #  convert it to an array
            if (!is_array($values)) $values = [$values];
            foreach ($values as $value) {
                # if value is an array, it contains attributes for this node [value, [attr=>attr_value]]
                $attrs = [];
                if (is_array($value)) {
                    list($value, $attrs) = $value;
                }
                $this->response->addChild($schema_node, $name, $value, $attrs);
            }
        }
    }

    private function createResumptionToken($delivered_records, $metadataPrefix='', $from='', $until='', $set='') {
        $values[] = $delivered_records;
        $values[] = $metadataPrefix;
        $values[] = $from;
        if (!$until) {
            $until = gmstrftime('%Y-%m-%dT%TZ', time());
        }
        $values[] = $until;
        $values[] = $set;
        $string = join(';', $values);
        $token = urlencode($string);
        return $token;
    }

    private function readResumptionToken($resumptionToken) {
        $string = urldecode($resumptionToken);
        $values = explode(';', $string);
        # if no cursor, no metadataPrefix or no until, this is a wrong token
        # TODO: test validity of dates
        if (!$values[0] || !$values[1] || !$values[3]) return false;
        return $values;
    }

    /**
     * All datestamps used in this system are GMT even
     * return value from database has no TZ information
     */
    private function formatDatestamp($datestamp) {
        return date("Y-m-d\TH:i:s\Z",strtotime($datestamp));
    }

    /**
     * The database uses datastamp without time-zone information.
     * It needs to clean all time-zone informaion from time string and reformat it
     */
    private function checkDateFormat($date) {
        $date = str_replace(array("T","Z")," ",$date);
        $time_val = strtotime($date);
        if(!$time_val) return false;
        if(strstr($date,":")) {
            return date("Y-m-d H:i:s",$time_val);
        } else {
            return date("Y-m-d",$time_val);
        }
    }
}
