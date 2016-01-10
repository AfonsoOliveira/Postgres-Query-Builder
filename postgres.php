<?php
	class Database {

		private $columns;
		private $values;
		private $select;
		private $table;
		private $from;
		private $join;
		private $where;
		private $update;
		private $order_by;
		private $group_by;
		private $limit;
		private $offset;
		private $having;
		private $query;
		private $num_rows;
		private $last_query;
		private static $join_types = array('INNER', 'LEFT', 'RIGHT', 'FULL OUTER');
		private static $operators = array('=', '!=', '>', '>=', '<', '<=', '<>', '!<', '!>', 'IN', 'NOT IN', 'NULL', 'NOT NULL', 'BETWEEN');

		public function __construct($data)
		{
			if(is_string($data))
			{
				pg_connect($data);
			}
			else if(is_array($data))
			{
				pg_connect('host=' . $data['host'] . ' port=' . $data['port'] . ' dbname=' . $data['dbname'] . ' user=' . $data['user'] . ' password=' . $data['password']);
			}
			else if(is_object($data))
			{
				pg_connect('host=' . $data->host . ' port=' . $data->port . ' dbname=' . $data->dbname . ' user=' . $data->user . ' password=' . $data->password);
			}
			else
			{
				throw new Exception('Dados de conexão com o banco de dados incorretos');
			}
		}

		protected function clearAttributes()
		{
			$not = array('last_query', 'operators', 'join_types');
			$attrs = get_object_vars($this);
			foreach ($attrs as $key => $value)
			{
				if(!in_array($key, $not))
				{
					$this->$key = null;
				}
			}
		}

		protected function escape_string($string)
		{
			if(is_array($string))
			{
				foreach ($string as $key => $value)
				{
					$string[$key] = pg_escape_string($value);
				}
			}
			else if(is_string($string))
			{
				$string = pg_escape_string($string);
			}
			return $string;
		}

		protected function _where($column, $value, $operator, $type, $escape)
		{
			if(!is_string($column))
			{
				throw new Exception('Nome da coluna não é string');
			}
			if(!in_array($operator, self::$operators))
			{
				throw new Exception('Operador não existente');
			}
			if(!in_array(strtoupper($operator), array('IN', 'NOT IN', 'BETWEEN')) && is_array($value))
			{
				throw new Exception('Valor não pode ser array neste tipo de operação');
			}
			if(strtoupper($operator) == 'BETWEEN' and !is_array($value))
			{
				throw new Exception('O range precisa ser array');
			}
			$type = (empty($this->where)) ? 'WHERE' : $type;
			$operator = (empty($operator)) ? '=' : $operator;
			$escape = (!is_bool($escape)) ? TRUE : $escape;
			if($escape)
			{
				$value = $this->escape_string($value);
			}
			if(in_array(strtoupper($operator), array('IN', 'NOT IN')))
			{
				$value = (is_array($value)) ? implode("','", $value) : $value;
				$where = " $type $column $operator ('$value')";
			}
			else if(in_array(strtoupper($operator), array('NULL', 'NOT NULL')))
			{
				$where = " $type $column IS $operator";
			}
			else if(strtoupper($operator) == 'BETWEEN')
			{
				$start = current($value);
				$end = end($value);
				$where = " $type $column $operator '$start' AND '$end'";
			}
			else
			{
				$where = " $type $column $operator '$value'";
			}
			$this->where .= $where;
			return $this;
		}

		protected function _select($columns, $type)
		{
			$columns = (is_array($columns)) ? implode(', ', $columns) : $columns;
			$array_columns = explode(', ', $columns);
			$select_type = (empty($this->select)) ? 'SELECT' : ',';
			foreach ($array_columns as $column)
			{
				if(preg_match('/^[a-zA-Z_][a-zA-Z0-9_]*$/', $column))
				{
					$this->columns[] = $column;
				}
			}
			if(empty($type))
			{
				$select = "$select_type $columns";
			}
			else
			{
				$select = "$select_type $type ($columns)";
			}
			$this->select .= $select;
			return $this;
		}

		public function begin()
		{
			pg_query('BEGIN');
			return $this;
		}

		public function rollback()
		{
			pg_query('ROLLBACK');
			return $this;
		}

		public function where($column, $value, $escape = TRUE)
		{
			$this->_where($column, $value, '=', 'AND', $escape);
			return $this;
		}

		public function or_where($column, $value, $escape = TRUE)
		{
			$this->_where($column, $value, '=', 'OR', $escape);
			return $this;
		}

		public function where_in($column, $values, $escape = TRUE)
		{
			$this->_where($column, $values, 'IN', 'AND', $escape);
			return $this;
		}

		public function or_where_in($column, $values, $escape = TRUE)
		{
			$this->_where($column, $values, 'IN', 'OR', $escape);
			return $this;
		}

		public function where_not_in($column, $values, $escape = TRUE)
		{
			$this->_where($column, $values, 'NOT IN', 'AND', $escape);
			return $this;
		}

		public function or_where_not_in($column, $values, $escape = TRUE)
		{
			$this->_where($column, $values, 'NOT IN', 'OR', $escape);
			return $this;
		}

		public function where_null($column, $escape = TRUE)
		{
			$this->_where($column, NULL, 'NULL', 'AND', $escape);
			return $this;
		}

		public function or_where_null($column, $escape = TRUE)
		{
			$this->_where($column, NULL, 'NULL', 'OR', $escape);
			return $this;
		}

		public function where_not_null($column, $escape = TRUE)
		{
			$this->_where($column, NULL, 'NOT NULL', 'AND', $escape);
			return $this;
		}

		public function or_where_not_null($column, $escape = TRUE)
		{
			$this->_where($column, NULL, 'NOT NULL', 'OR', $escape);
			return $this;
		}

		public function where_between($column, $range, $escape = TRUE)
		{
			$this->_where($column, $range, 'BETWEEN', 'AND', $escape);
			return $this;
		}

		public function or_where_between($column, $range, $escape = TRUE)
		{
			$this->_where($column, $range, 'BETWEEN', 'OR', $escape);
			return $this;
		}

		public function group_start()
		{
			$this->where .= '(';
		}

		public function group_end()
		{
			$this->where .= ')';
		}

		public function group_by($columns)
		{
			$columns = (is_array($columns)) ? implode(', ', $columns) : $columns;
			$this->group_by = ' GROUP BY ' . $columns;
			return $this;
		}

		public function order_by($columns, $order = 'ASC')
		{
			$columns = (is_array($columns)) ? implode(', ', $columns) : $columns;
			if(empty($this->order_by))
			{
				$this->order_by = " ORDER BY $columns $order";
			}
			else
			{
				$this->order_by .= ", $columns $order";
			}
			return $this;
		}

		public function limit($limit)
		{
			if(!is_numeric($limit))
			{
				throw new Exception('Valor do limite não é numérico');
			}
			$this->limit = " LIMIT $limit";
			return $this;
		}

		public function offset($offset)
		{
			if(!is_numeric($offset))
			{
				throw new Exception('Valor do offset não é numérico');
			}
			$this->offset = " OFFSET $offset";
			return $this;
		}

		public function having($column, $having, $operator = '=')
		{
			if(!is_string($column))
			{
				throw new Exception('Nome da coluna não é string');
			}
			if(!in_array($operator, self::$operators))
			{
				throw new Exception('Operador não existente');
			}
			if(empty($this->having))
			{
				$this->having = " HAVING $column $operator $having";
			}
			else
			{
				$this->having .= " AND $column $operator $having";
			}
		}

		public function or_having($column, $having, $operator = '=')
		{
			if(!is_string($column))
			{
				throw new Exception('Nome da coluna não é string');
			}
			if(!in_array($operator, self::$operators))
			{
				throw new Exception('Operador não existente');
			}
			$this->having .= " OR $column $operator $having";
		}

		public function select($columns = '*', $type = NULL)
		{
			$this->_select($columns, $type);
			return $this;
		}

		public function select_count($column = '*')
		{
			$this->_select($column, 'COUNT');
			return $this;
		}

		public function select_max($column)
		{
			$this->_select($column, 'MAX');
			return $this;
		}

		public function select_min($column)
		{
			$this->_select($column, 'MIN');
			return $this;
		}

		public function select_sum($column)
		{
			$this->_select($column, 'SUM');
			return $this;
		}

		public function select_avg($column)
		{
			$this->_select($column, 'AVG');
			return $this;
		}

		public function from($from)
		{
			if(!is_string($from))
			{
				throw new Exception('Nome da tabela não é string');
			}
			$this->from = " FROM $from";
			$this->table = $from;
			return $this;
		}

		public function join($table, $union_column, $type = 'INNER')
		{
			if(!is_string($table))
			{
				throw new Exception('Nome da tabela não é string');
			}
			if(!in_array(strtoupper($type), self::$join_types))
			{
				throw new Exception('Tipo de join não existente');
			}
			$union = (count(explode(' ', $union_column)) == 1) ? "USING($union_column)" : "ON($union_column)";
			$this->join .= " $type JOIN $table $union";
			return $this;
		}

		public function get($table = NULL)
		{
			if(empty($this->select))
			{
				$this->select();
			}
			if(!empty($table))
			{
				$this->from($table);
			}
			if(is_array($this->columns))
			{
				foreach ($this->columns as $column)
				{
					$column_verification = pg_query("SELECT column_name FROM information_schema.columns WHERE table_name='$this->table' and column_name='$column'");
					if(!pg_num_rows($column_verification))
					{
						throw new Exception('Coluna $column não existe na tabela $this->table');
					}
				}
			}
			$table_verification = pg_query("SELECT relname FROM pg_class WHERE relname = '$this->table'");
			if(pg_num_rows($table_verification))
			{
				$query = implode(array(
					$this->select,
					$this->from,
					$this->join,
					$this->where,
					$this->group_by,
					$this->order_by,
					$this->limit,
					$this->offset,
					$this->having
				));
				$this->query = pg_query($query);
				$this->num_rows = pg_num_rows($this->query);
				$this->last_query = $query;
			}
			else
			{
				throw new Exception('Tabela não existe');
			}
			return $this;
		}

		public function num_rows()
		{
			return $this->num_rows;
		}

		public function row()
		{
			$this->clearAttributes();
			return pg_fetch_object($this->query);
		}

		public function row_array()
		{
			$this->clearAttributes();
			return pg_fetch_array($this->query);
		}

		public function result()
		{
			$data = array();
			while($result = pg_fetch_object($this->query))
			{
				$data[] = $result;
			}
			$this->clearAttributes();
			return $data;
		}

		public function result_array()
		{
			$data = array();
			while($result = pg_fetch_assoc($this->query))
			{
				$data[] = $result;
			}
			$this->clearAttributes();
			return $data;
		}

		public function delete($table)
		{
			if(!is_string($table))
			{
				throw new Exception('Nome da tabela não é string');
			}
			$this->table = $table;
			$table_verification = pg_query("SELECT relname FROM pg_class WHERE relname = '$this->table'");
			if(pg_num_rows($table_verification))
			{
				$query = "DELETE FROM $table $this->where";
				$this->query = pg_query($query);
				$this->last_query = $query;
				$this->clearAttributes();
			}
			else
			{
				throw new Exception('Tabela não existe');
			}
		}

		public function insert($table, $items, $escape = TRUE)
		{
			if(!is_string($table))
			{
				throw new Exception('Nome da tabela não é string');
			}
			if(!is_array($items))
			{
				throw new Exception('Itens do insert não estão no formato array');
			}
			$this->table = $table;
			$escape = (!is_bool($escape)) ? TRUE : $escape;
			if($escape)
			{
				$items = $this->escape_string($items);
			}
			$table_verification = pg_query("SELECT relname FROM pg_class WHERE relname = '$this->table'");
			if(pg_num_rows($table_verification))
			{
				$columns = implode(', ', array_keys($items));
				$values = "'".implode("', '", $items)."'";
				$query = "INSERT INTO $table ($columns) VALUES ($values)";
				$this->query = pg_query($query);
				$this->last_query = $query;
				$this->clearAttributes();
				return $this->query;
			}
			else
			{
				throw new Exception('Tabela não existe');
			}
		}

		public function update($table, $items, $escape = TRUE)
		{
			if(!is_string($table))
			{
				throw new Exception('Nome da tabela não é string');
			}
			if(!is_array($items))
			{
				throw new Exception('Itens do update não estão no formato array');
			}
			$this->table = $table;
			$escape = (!is_bool($escape)) ? TRUE : $escape;
			if($escape)
			{
				$items = $this->escape_string($items);
			}
			$table_verification = pg_query("SELECT relname FROM pg_class WHERE relname = '$this->table'");
			if(pg_num_rows($table_verification))
			{
				foreach ($items as $column => $value)
				{
					$this->update[] = "$column = '$value'";
				}
				$this->update = implode(', ', $this->update);
				$query = "UPDATE $table SET $this->update $this->where";
				$this->query = pg_query($query);
				$this->last_query = $query;
				$this->clearAttributes();
				return $this->query;
			}
			else
			{
				throw new Exception('Tabela não existe');
			}
		}

		public function query($query, $escape = TRUE)
		{
			$escape = (!is_bool($escape)) ? TRUE : $escape;
			if($escape)
			{
				$query = $this->escape_string($query);
			}
			$this->query = pg_query($query);
			return $this;
		}

		public function getLastQuery()
		{
			return $this->last_query;
		}

	}