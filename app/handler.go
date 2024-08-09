package main

import (
	"errors"
	"slices"
	"strconv"
	"strings"
)

type Query struct {
	stmt     *SelectStatement
	db       *SQLite
	rootPage *Page
}

type SelectStatement struct {
	cols     []string
	function string
	from     string
	where    *Where
}

type Where struct {
	left  string
	right string
	op    string
}

// Parser ---------------------------------------------------------------------
func ParseSelectStatement(input string) (*SelectStatement, error) {
	_, input = ReadIncludingString(input, "select")
	if input == "" {
		return nil, errors.New("expected SELECT")
	}

	cols, function, err := ParseExpr(input)
	if err != nil {
		return nil, err
	}

	_, input = ReadIncludingString(input, "from")
	if input == "" {
		return nil, errors.New("expected FROM")
	}

	from, err := ParseFrom(input)
	if err != nil {
		return nil, err
	}

	_, input = ReadIncludingString(input, "where")
	var where *Where
	if input != "" {
		where, err = ParseWhere(input)
		if err != nil {
			return nil, err
		}
	}

	return &SelectStatement{
		cols,
		function,
		from,
		where,
	}, nil
}

func ReadBeforeString(input string, delim string) (string, string) {
	var delimIdx int
	if delim == "END" {
		delimIdx = len(input)
	} else {
		delimIdx = strings.Index(input, delim)
		if delimIdx == -1 {
			delimIdx = len(input)
		}
	}
	result := strings.Trim(input[:delimIdx], " ")
	input = strings.Trim(input[delimIdx:], " ")
	return result, input
}

func ReadIncludingString(input string, delim string) (string, string) {
	result, input := ReadBeforeString(input, delim)
	if input == "" {
		return "", ""
	}
	result = result + input[:len(delim)]
	input = input[len(delim):]
	return result, input
}

func ParseExpr(input string) ([]string, string, error) {
	expr, _ := ReadBeforeString(input, "from")

	if strings.Contains(expr, "(") {
		functionName, args := ReadBeforeString(expr, "(")
		switch functionName {
		case "count":
			insideExpr := args[1 : len(args)-1]
			args := strings.Split(insideExpr, ",")
			return args, "COUNT", nil
		default:
			return nil, "", errors.New("function not yet implemented")
		}
	}
	return strings.Split(expr, ", "), "", nil
}

func ParseFrom(input string) (string, error) {
	keyword, _ := ReadBeforeString(input, "where")
	return strings.Trim(keyword, " "), nil
}

func ParseWhere(input string) (*Where, error) {
	args := strings.SplitAfterN(strings.Trim(input, " "), " ", 3)
	for i, arg := range args {
		args[i] = strings.TrimSpace(arg)
	}
	return &Where{
		left:  args[0],
		right: strings.Trim(args[2], "'"),
		op:    args[1],
	}, nil
}

// ----------------------------------------------------------------------------

func HandleCommand(input string, db *SQLite) ([]string, error) {
	stmt, err := ParseSelectStatement(strings.ToLower(input))
	if err != nil {
		return nil, err
	}

	var rootPage *Page
	tableName := stmt.from
	rootPage, err = stmt.CheckForIndexPage(db)
	if err != nil {
		rootPage, err = db.GetRootPageByName(tableName)
		if err != nil {
			return nil, err
		}
	}

	query := &Query{
		stmt,
		db,
		rootPage,
	}

	if stmt.function == "" {
		return query.EvaluateStmt()
	}

	result, err := query.EvaluateStmt()
	if err != nil {
		return nil, err
	}

	return query.EvaluateFunc(result)
}

// Handler ---------------------------------------------------------------------
func (q *Query) EvaluateStmt() ([]string, error) {
	tableName := q.stmt.from
	cols := q.db.GetTableColNames(tableName)

	result := q.rootPage.GetAllRows(cols)
	result = append(result, cols)

	if len(q.stmt.cols) == 1 && q.stmt.cols[0] == "*" {
		return FlattenResult(result[:len(result)-1]), nil
	}

	result, err := q.EvaluateWhere(result)
	if err != nil {
		return nil, err
	}

	return FlattenResult(q.stmt.SelectCols(result)), nil
}

func (stmt *SelectStatement) SelectCols(result [][]string) [][]string {
	colNames := result[len(result)-1]
	result = result[:len(result)-1]

	selectedResult := make([][]string, len(result))
	colsWanted := stmt.cols

	for _, colName := range colsWanted {
		colIdx := slices.IndexFunc(colNames, func(col string) bool {
			return strings.Contains(col, colName)
		})
		if colIdx == -1 {
			continue
		}

		for i, row := range result {
			if selectedResult[i] == nil {
				selectedResult[i] = make([]string, 0)
			}
			selectedResult[i] = append(selectedResult[i], row[colIdx])
		}
	}
	return selectedResult
}

func FlattenResult(rows [][]string) []string {
	result := make([]string, len(rows))
	for i, row := range rows {
		result[i] = strings.Join(row, "|")
	}
	return result
}

func (q *Query) EvaluateFunc(result []string) ([]string, error) {
	switch q.stmt.function {
	case "COUNT":
		return []string{strconv.Itoa(len(result))}, nil
	default:
		return nil, errors.New("function not yet implemented")
	}
}

func (q *Query) EvaluateWhere(result [][]string) ([][]string, error) {
	if q.stmt.where == nil {
		return result, nil
	}
	switch q.stmt.where.op {
	case "=":
		return q.stmt.where.FilterEqual(result)
	default:
		return nil, errors.New("WHERE operator not yet implemented")
	}
}

func (stmt *SelectStatement) CheckForIndexPage(db *SQLite) (*Page, error) {
	if stmt.where == nil {
		return nil, errors.New("no where clause")
	}

	idxPageName := db.GetIndexPageName(stmt.where.left)
	if idxPageName == "" {
		return nil, errors.New("no index page found for where clause")
	}

	idxPage, err := db.GetRootPageByName(idxPageName, stmt.where.right)
	if err != nil {
		return nil, err
	}

	rowIds := idxPage.GetFilteredRowIDs()
	tableName := stmt.from
	result, err := db.GetRootPageByName(tableName, rowIds)

	return result, err
}

func (where *Where) FilterEqual(result [][]string) ([][]string, error) {
	colNames := result[len(result)-1]
	result = result[:len(result)-1]

	var colIdx int
	for i, colName := range colNames {
		if strings.Contains(colName, where.left) {
			colIdx = i
			break
		}
	}

	filteredResult := [][]string{}
	for _, row := range result {
		if strings.ToLower(row[colIdx]) == where.right {
			filteredResult = append(filteredResult, row)
		}
	}
	filteredResult = append(filteredResult, colNames)
	return filteredResult, nil
}

// ----------------------------------------------------------------------------
