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

            /**
             * @var Cassandra\Cluster
             */
            public $_cluster;

            /**
             * @var string
             */
            public $keyspace;

            /**
             * @var Cassandra\Session
             */
            public $_session;

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
                    $this->keyspace = $keyspace;
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
                foreach ($result as $row) {
                    foreach ($row as $key => $val) {
                        if (is_a($val, 'Cassandra\Value')) {
                            $row[$key] = $this->convert_cassandra_value($val);
                        }
                    }
                    $this->_rows[] = $row;
                }
                $this->num_rows = count($this->_rows);
            }

            /**
             * @param \Cassandra\Value $val
             * @return bool|int|string
             */
            function convert_cassandra_value($val)
            {
                $type = $val->type();
                if (is_a($type, 'Cassandra\Type\Scalar')) {
                    switch ($type->name()) {
                        case 'bigint':
                            /** @var Cassandra\Bigint $val */
                            return $val->toInt();
                            break;

                        case 'timestamp':
                            /** @var Cassandra\Timestamp $val */
                            return date('Y-m-d H:i:s', $val->time());
                            break;
                    }
                }
                return (string)$val;
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
            try {
                $return = $this->_conn->_db->selectCollection($table)->insert($set);
                $this->_conn->errno = $return['code'];
                $this->_conn->error = $return['err'];
                $this->_conn->last_id = $set['_id'];
                return !$return['err'];
            } catch (Exception $ex) {
                $this->_conn->error = $ex->getMessage();
                return false;
            }
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
        $tables = $connection->_session->schema()->keyspace($connection->keyspace)->tables();
        foreach ($tables as $t) {
            $ret[$t->name()] = 'table';
        }
        return $ret;
    }

    function table_status($name = "", $fast = false)
    {
        $return = array();
        foreach (tables_list() as $table => $type) {
            $return[$table] = array("Name" => $table);
            if ($name == $table) {
                return $return[$table];
            }
        }
        return $return;
    }

    function information_schema()
    {
    }

    function is_view($table_status)
    {
    }

    function drop_databases($databases)
    {
        /** @var Min_DB */
        global $connection;

        foreach ($databases as $db) {
            $response = $connection->_link->selectDB($db)->drop();
            if (!$response['ok']) {
                return false;
            }
        }
        return true;
    }

    function indexes($table, $connection2 = null)
    {
        /** @var Min_DB */
        global $connection;

        $ret = array();
        /** @var Cassandra\Column[] $cols */
        $cols = $connection->_session->schema()->keyspace($connection->keyspace)->table($table)->columns();
        foreach ($cols as $col) {
            $ind = $col->indexName();
            $io = $col->indexOptions();
            if ($ind || $io) {
                $ret[$col->name() . ' ' . $ind . '/' . $io] = array(
                    'type' => 'test',
                );
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
        $cols = $connection->_session->schema()->keyspace($connection->keyspace)->table($table)->columns();
        foreach ($cols as $col) {
            $ret[$col->name()] = array(
                'field' => $col->name(),
                'full_type' => $col->type()->name(),
                'type' => $col->type()->name(),
                // 'primary' => true,
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

    function found_rows($table_status, $where)
    {
        /** @var Min_DB */
        global $connection;

        //! don't call count_rows()
        return null;
    }

    function alter_table($table, $name, $fields, $foreign, $comment, $engine, $collation, $auto_increment, $partitioning)
    {
        /** @var Min_DB */
        global $connection;

        if ($table == "") {
            $connection->_db->createCollection($name);
            return true;
        }
    }

    function drop_tables($tables)
    {
        /** @var Min_DB */
        global $connection;

        foreach ($tables as $table) {
            $response = $connection->_db->selectCollection($table)->drop();
            if (!$response['ok']) {
                return false;
            }
        }
        return true;
    }

    function truncate_tables($tables)
    {
        /** @var Min_DB */
        global $connection;

        foreach ($tables as $table) {
            $response = $connection->_db->selectCollection($table)->remove();
            if (!$response['ok']) {
                return false;
            }
        }
        return true;
    }

    function alter_indexes($table, $alter)
    {
        /** @var Min_DB */
        global $connection;

        foreach ($alter as $val) {
            list($type, $name, $set) = $val;
            if ($set == "DROP") {
                $return = $connection->_db->command(array("deleteIndexes" => $table, "index" => $name));
            } else {
                $columns = array();
                foreach ($set as $column) {
                    $column = preg_replace('~ DESC$~', '', $column, 1, $count);
                    $columns[$column] = ($count ? -1 : 1);
                }
                $return = $connection->_db->selectCollection($table)->ensureIndex($columns, array(
                    "unique" => ($type == "UNIQUE"),
                    "name" => $name,
                    //! "sparse"
                ));
            }
            if ($return['errmsg']) {
                $connection->error = $return['errmsg'];
                return false;
            }
        }
        return true;
    }

    function last_id()
    {
        /** @var Min_DB */
        global $connection;

        return $connection->last_id;
    }

    function table($idf)
    {
        return $idf;
    }

    function idf_escape($idf)
    {
        return '"' . $idf . '"';
    }

    function support($feature)
    {
        return preg_match("~database|indexes|sql~", $feature);
    }

    $jush = "cassandra";
    $operators = array("=", "<", ">", "<=", ">=", "CONTAINS", "CONTAINS KEY");
    $functions = array();
    $grouping = array();
    $edit_functions = array(array("json"));
}
