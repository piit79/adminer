<?php
$drivers["cassandra"] = "Cassandra (alpha)";

if (isset($_GET["cassandra"])) {
    $possible_drivers = array("cassandra");
    define("DRIVER", "cassandra");

    if (class_exists('Cassandra')) {
        class Min_DB
        {
            public $extension = "Cassandra";
            public $error;
            public $last_id;
            public $_result;

            /**
             * @var Cassandra\Cluster
             */
            public $_cluster;

            /**
             * @var Cassandra\Session
             */
            public $_session;

            /**
             * @var Cassandra\Keyspace
             */
            public $_keyspace;


            function connect($server, $username, $password)
            {
                global $adminer;

                $db = $adminer->database();
                $options = array();
                if ($db != "") {
                    $options["db"] = $db;
                }
                try {
                    $this->_cluster = Cassandra::cluster()
                        ->withContactPoints($server)
                        ->withCredentials($username, $password)
                        ->build();

                    return $this->select_db();
                } catch (Exception $ex) {
                    $this->error = $ex->getMessage();

                    return false;
                }
            }


            function query($query)
            {
                $res = $this->_session->execute(new Cassandra\SimpleStatement($query));

                return new Min_Result($res);
            }


            /** Send query with more resultsets
             *
             * @param string
             * @return bool
             */
            function multi_query($query)
            {
                return $this->_result = $this->query($query);
            }


            /** Get current resultset
             *
             * @return Min_Result
             */
            function store_result()
            {
                return $this->_result;
            }


            /** Fetch next resultset
             *
             * @return bool
             */
            function next_result()
            {
                return false;
            }


            function result($query, $field = 0)
            {
                $result = $this->query($query);
                if (!is_object($result)) {
                    return false;
                }
                $row = $result->fetch_row();

                return $row[$field];
            }


            function select_db($keyspace = null)
            {
                try {
                    $this->_session = $this->_cluster->connect($keyspace);
                    $this->_keyspace = $this->_session->schema()->keyspace($keyspace);

                    return true;
                } catch (Exception $ex) {
                    $this->error = $ex->getMessage();

                    return false;
                }
            }


            function quote($string)
            {
                return "'" . $this->escape_string($string) . "'";
            }


            function escape_string($string)
            {
                return str_replace("'", "''", $string);
            }

        }

        class Min_Result
        {
            public $num_rows;
            public $_rows = array();
            public $_offset = 0;
            public $_charset = array();


            function __construct($result)
            {
//                echo "<pre>RES: " . var_export($result, true) . "\n</pre>";
                foreach ($result as $row) {
                    foreach ($row as $key => $val) {
                        if (is_a($val, 'Cassandra\Value')) {
                            $row[$key] = $this->convert_cassandra_value($val);
                        }
                    }
                    $this->_rows[] = $row;
                }
//                echo "<pre>ROWS: " . var_export($this->_rows, true) . "\n</pre>";
                $this->num_rows = count($this->_rows);
            }


            /**
             * @param \Cassandra\Value $value
             * @return bool|int|string
             */
            function convert_cassandra_value($value)
            {
                if ($value instanceof \Cassandra\Tinyint
                    || $value instanceof \Cassandra\Smallint
                    || $value instanceof \Cassandra\Bigint
                ) {
                    return $value->value();
                } elseif ($value instanceof \Cassandra\Decimal
                    || $value instanceof \Cassandra\Float
                ) {
                    return $value->toDouble();
                } elseif ($value instanceof \Cassandra\Timeuuid) {
                    return $value . ' [' . $value->toDateTime()->format('Y-m-d H:i:s') . ']';
                } elseif ($value instanceof \Cassandra\Timestamp) {
                    return $value->toDateTime()->format('Y-m-d H:i:s');
                } elseif ($value instanceof \Cassandra\Date) {
                    return $value->toDateTime()->format('Y-m-d');
                } elseif ($value instanceof \Cassandra\Time) {
                    $ns = $value->nanoseconds();
                    return gmdate('H:i:s', floor($ns / 1e6)) . sprintf('.%06d', $ns % 1e6);
                } elseif ($value instanceof \Cassandra\Inet) {
                    return $value->address();
                } elseif ($value instanceof \Cassandra\Map) {
                    /** @var Cassandra\Type\Map $type */
                    $keyVals = array();
                    foreach ($value as $key => $val) {
                        if (is_string($key)) {
                            $key = "'" . $key . "'";
                        }
                        if (is_string($val)) {
                            $val = "'" . $val . "'";
                        }
                        $keyVals[] = $key . ': ' . $val;
                    }
                    $ret = "{ " . implode(",\n", $keyVals) . " }";

                    return $ret;
                }

                return (string)$value;
            }


            function fetch_assoc()
            {
                $row = current($this->_rows);
                next($this->_rows);

                return $row;
            }


            function fetch_row()
            {
                $ret = $this->fetch_assoc();
                if (!$ret) {
                    return $ret;
                }

                return array_values($ret);
            }


            function fetch_field()
            {
                $keys = array_keys($this->_rows[0]);
                $name = $keys[$this->_offset++];

                return (object)array(
                    'name' => $name,
                );
            }

        }
    }

    class Min_Driver extends Min_SQL
    {
        public $primary = "_id";


        function select($table, $select, $where, $group, $order = array(), $limit = 1, $page = 0, $print = false)
        {
            global $adminer;

            $fields = implode(', ', $select);
            $query = "SELECT $fields FROM $table";
            if ($where) {
                $query .= ' WHERE ' . implode(' AND ', $where);
            }
            //if ($order) {
            //    $query .= ' ORDER BY ' . implode(', ', $order);
            //}
            //echo "<pre>SELECT: " . var_export($select, true) . "\n</pre>";
            //echo "<pre>WHERE:  " . var_export($where, true) . "\n</pre>";
            //echo "<pre>GROUP:  " . var_export($group, true) . "\n</pre>";
            //echo "<pre>ORDER:  " . var_export($order, true) . "\n</pre>";
            //echo "<pre>LIMIT:  $limit\n</pre>";
            //echo "<pre>PAGE:   $page\n</pre>";
            //echo "<pre>QUERY:  $query\n</pre>";
            $start = microtime(true);
            $res_obj = $this->_conn->_session->execute(new Cassandra\SimpleStatement($query));
            if ($print) {
                echo $adminer->selectQuery($query, format_time($start));
            }
            $res = array();
            foreach ($res_obj as $row) {
                $res[] = $row;
            }
            $res_limited = array_slice($res, $page * $limit, $limit);

            return new Min_Result($res_limited);
        }


        function insert($table, $set)
        {
            return false;
        }

    }


    function connect()
    {
        global $adminer;

        $connection = new Min_DB;
        $credentials = $adminer->credentials();
        if ($connection->connect($credentials[0], $credentials[1], $credentials[2])) {
            return $connection;
        }

        return $connection->error;
    }

    function error()
    {
        global $connection;

        return h($connection->error);
    }

    function logged_user()
    {
        global $adminer;
        $credentials = $adminer->credentials();

        return $credentials[1];
    }

    function get_databases($flush)
    {
        /** @var Min_DB */
        global $connection;

        $ret = array();
        /** @var Cassandra\Keyspace[] $keyspaces */
        $keyspaces = $connection->_session->schema()->keyspaces();
        foreach ($keyspaces as $ks) {
            $ret[] = $ks->name();
        }

        return $ret;
    }

    function collations()
    {
        return array();
    }

    function db_collation($db, $collations)
    {
    }

    function count_tables($keyspaces)
    {
        /** @var Min_DB */
        global $connection;

        $return = array();
        foreach ($keyspaces as $ks) {
            $return[$ks] = count($connection->_session->schema()->keyspace($ks)->tables());
        }

        return $return;
    }

    function tables_list()
    {
        /** @var Min_DB */
        global $connection;

        $ret = array();
        /** @var Cassandra\Table[] $tables */
        $tables = $connection->_keyspace->tables();
        foreach ($tables as $tbl) {
            $ret[$tbl->name()] = 'table';
        }

        return $ret;
    }

    function table_status($name = "", $fast = false)
    {
        /** @var Min_DB */
        global $connection;

        $ret = array();
        // return info about a single table?
        if ($name) {
            $tbl = $connection->_keyspace->table($name);

            return array(
                'Name'    => $tbl->name(),
                'Comment' => $tbl->comment(),
            );
        }
        // return info about all tables
        $tables = $connection->_keyspace->tables();
        foreach ($tables as $tbl) {
            $ret[$tbl->name()] = array(
                'Name'    => $tbl->name(),
                'Comment' => $tbl->comment(),
            );
        }

        return $ret;
    }

    function information_schema()
    {
    }

    function is_view($table_status)
    {
        return false;
    }

    function drop_databases($databases)
    {
        $ret = true;
        foreach ($databases as $ks) {
            $ret = $ret && queries('DROP KEYSPACE "$ks"');
        }

        return $ret;
    }

    function indexes($table, $connection2 = null)
    {
        /** @var Min_DB */
        global $connection;

        $ret = array();

        $clustKeyCols = $connection->_keyspace->table($table)->clusteringKey();
        $clustOrder = $connection->_keyspace->table($table)->clusteringOrder();
        $ckInfo = array(
            'type'    => 'CLUSTERING ORDER',
            'columns' => array(),
            'lengths' => array(),
            'descs'   => array(),
        );
        if (count($clustKeyCols)) {
            foreach ($clustKeyCols as $i => $col) {
                $ckInfo['columns'][$col->name()] = $col->name();
                if (isset($clustOrder[$i]) && strtoupper($clustOrder[$i]) == 'DESC') {
                    $ckInfo['descs'][$col->name()] = 1;
                }
            }
            $ret['CLUSTERING ORDER'] = $ckInfo;
        }

        $partKeyCols = $connection->_keyspace->table($table)->partitionKey();
        if (count($partKeyCols)) {
            $pkInfo = array(
                'type'    => 'PRIMARY',
                'columns' => array(),
                'lengths' => array(),
                'descs'   => array(),
            );
            $partCols = array();
            foreach ($partKeyCols as $col) {
                $partCols[] = $col->name();
            }
            $pk = implode($partCols, ', ');
            if (count($partCols) > 1) {
                $pk = '(' . $pk . ')';
            }
            $pkInfo['columns'][$pk] = $pk;
            $pkInfo['columns'] = array_merge($pkInfo['columns'], $ckInfo['columns']);
            $ret['PRIMARY'] = $pkInfo;
        }

        /** @var Cassandra\Column[] $cols */
        $cols = $connection->_keyspace->table($table)->columns();
        foreach ($cols as $col) {
            $indexName = $col->indexName();
            if ($indexName) {
                $indexOptions = $col->indexOptions();
                if (!isset($ret[$indexName])) {
                    $ret[$indexName] = array(
                        'type'    => 'INDEX',
                        'columns' => array(),
                        'lengths' => array(),
                        'descs'   => array(),
                    );
                }
                $ret[$indexName]['columns'][$col->name()] = $col->name();
                if ($col->isReversed()) {
                    $ret[$indexName][$col->name()] = 1;
                }
            }
        }

        return $ret;
    }

    function fields($table)
    {
        /** @var Min_DB */
        global $connection;

        $ret = array();
        /** @var Cassandra\Column[] $cols */
        $cols = $connection->_keyspace->table($table)->columns();
        foreach ($cols as $col) {
            $type = $col->type();
            $typeName = $type->name();
            if ($type instanceof \Cassandra\Type\Collection
                || $type instanceof \Cassandra\Type\Set
            ) {
                $typeName .= '<' . $type->valueType()->name() . '>';
            } elseif ($type instanceof \Cassandra\Type\Map) {
                $typeName .= '<' . $type->keyType()->name() . ', ' . $type->valueType()->name() . '>';
            } elseif ($type instanceof \Cassandra\Type\Tuple) {
                $types = array();
                foreach ($type->types() as $t) {
                    $types[] = $t->name();
                }
                $typeName .= '<' .  implode(', ', $types) . '>';
            }
            $ret[$col->name()] = array(
                'field'      => $col->name(),
                'full_type'  => $typeName,
                'type'       => $typeName,
                'privileges' => array('insert' => 1, 'select' => 1, 'update' => 1),
            );
        }

        return $ret;
    }

    function convert_field($field)
    {
    }

    function unconvert_field($field, $return)
    {
//        echo "<pre>FIELD: " . var_export($field, true) . "</pre>";
        if ($field['type'] != 'varchar') {
            if (
                substr($return, 0, 1) == "'"
                && substr($return, -1) == "'"
            ) {
                return substr($return, 1, -1);
            }
        }

        return $return;
    }

    function foreign_keys($table)
    {
        return array();
    }

    function fk_support($table_status)
    {
    }

    function engines()
    {
        return array();
    }

    /** Explain select
     *
     * @param Min_DB
     * @param string
     * @return Min_Result|false
     */
    function explain($connection, $query)
    {
        return false;
    }

    function found_rows($table_status, $where)
    {
        /** @var Min_DB */
        global $connection;

        return null;
    }

    function alter_table($table, $name, $fields, $foreign, $comment, $engine, $collation, $auto_increment, $partitioning)
    {
        /** @var Min_DB */
        global $connection;

        return false;
    }

    function drop_tables($tables)
    {
        $ret = true;
        foreach ($tables as $tbl) {
            $ret = $ret && queries('DROP TABLE ' . table($tbl));
        }

        return $ret;
    }

    function truncate_tables($tables)
    {
        $ret = true;
        foreach ($tables as $tbl) {
            $ret = $ret && queries('TRUNCATE TABLE ' . table($tbl));
        }

        return $ret;
    }

    function alter_indexes($table, $alter)
    {
        /** @var Min_DB */
        global $connection;

        return false;
    }

    function last_id()
    {
        /** @var Min_DB */
        global $connection;

        return $connection->last_id;
    }

    function table($idf)
    {
        return idf_escape($idf);
    }

    function idf_escape($idf)
    {
        return '"' . $idf . '"';
    }

    function support($feature)
    {
        return preg_match("~database|indexes|columns|sql~", $feature);
    }

    $jush = "cassandra";
    $types = array(); ///< @var array ($type => $maximum_unsigned_length, ...)
    $structured_types = array(); ///< @var array ($description => array($type, ...), ...)
    foreach (array(
                 lang('Numbers')       => array("tinyint" => 1, "smallint" => 1, "int" => 1, "bigint" => 1,
                                                "varint" => 1, "decimal" => 1, "float" => 1, "double" => 1,
                                                "counter" => 1),
                 lang('Date and time') => array("timeuuid" => 1, "date" => 1, "time" => 1, "timestamp" => 1),
                 lang('Strings')       => array("varchar" => 1, "text" => 1, "ascii" => 1),
                 lang('Binary')        => array("blob" => 1),
                 lang('Collections')   => array("list" => 1, "set" => 1, "map" => 1, "tuple" => 1),
                 lang('Other')         => array("boolean" => 1, "inet" => 1),
             ) as $key => $val) {
        $types += $val;
        $structured_types[$key] = array_keys($val);
    }
    $operators = array("=", "<", ">", "<=", ">=", "CONTAINS", "CONTAINS KEY");
    $functions = array();
    $grouping = array();
    $edit_functions = array(array("json"));
}
