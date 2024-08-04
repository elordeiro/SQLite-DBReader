package main

import (
	"errors"
	"strconv"
	"strings"
	// "github.com/xwb1989/sqlparser"
)

type SelectStatement struct {
	Exprs *Expr
	From  *From
	Where *Where
}

type Expr struct {
	Args     Args
	Function *Function
}

type Function struct {
	Name string
	Args Args
}

type Args []string

type From struct {
	Exprs *Expr
}

type Where struct {
	Exprs *Expr
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

func ParseSelectStatement(input string) (*SelectStatement, error) {
	_, input = ReadIncludingString(input, "select")
	if input == "" {
		return nil, errors.New("expected SELECT")
	}

	exprs, err := ParseExpr(input)
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
		Exprs: exprs,
		From:  from,
		Where: where,
	}, nil
}

func ParseExpr(input string) (*Expr, error) {
	expr, _ := ReadBeforeString(input, "from")

	if strings.Contains(expr, "(") {
		functionName, args := ReadBeforeString(expr, "(")
		switch functionName {
		case "count":
			insideExpr := args[1 : len(args)-1]
			args := strings.Split(insideExpr, ",")
			return &Expr{
				Args: strings.Split(insideExpr, ", "),
				Function: &Function{
					Name: "COUNT",
					Args: args,
				},
			}, nil
		default:
			return nil, errors.New("function not yet implemented")
		}
	}
	return &Expr{
		Args: strings.Split(expr, ", "),
	}, nil
}

func ParseFrom(input string) (*From, error) {
	keyword, _ := ReadBeforeString(input, "where")

	return &From{
		Exprs: &Expr{
			Args: strings.Split(strings.Trim(keyword, " "), " "),
		},
	}, nil

}

func ParseWhere(input string) (*Where, error) {
	return &Where{
		Exprs: &Expr{
			Args: strings.Split(strings.Trim(input, " "), " "),
		},
	}, nil
}

func (page *Page) HandleCommand(input string) ([]string, error) {
	stmt, err := ParseSelectStatement(strings.ToLower(input))
	if err != nil {
		return nil, err
	}

	if len(stmt.From.Exprs.Args) != 1 {
		return nil, errors.New("only FROM argument is currently supported")
	}

	table, err := page.GetTablebyName(stmt.From.Exprs.Args[0])
	if err != nil {
		return nil, err
	}

	if stmt.Exprs.Function == nil {
		return EvaluateStatement(page, table, stmt)
	}
	result, err := EvaluateStatement(page, table, stmt)
	if err != nil {
		return nil, err
	}
	return EvaluateFunction(result, stmt)
}

func EvaluateStatement(rootPage *Page, table *Page, stmt *SelectStatement) ([]string, error) {
	if stmt.Exprs.Args[0] == "*" {
		var result []string
		for _, cell := range table.Cells {
			row := ""
			for _, keys := range cell.Record.Keys {
				row += string(keys) + " "
			}
			result = append(result, row)
		}
		return result, nil
	}

	tableName := stmt.From.Exprs.Args[0]
	cols, _ := rootPage.GetTableColumns(tableName)

	rowCount := table.Header.CellCount
	result := make([][]string, rowCount+1)
	for _, colName := range stmt.Exprs.Args {
		var colNum int
		for i, col := range cols {
			if strings.Contains(col, colName) {
				colNum = i
				break
			}
		}

		for j, cell := range table.Cells {
			row := cell.Record.Keys[colNum]
			if result[j] == nil {
				result[j] = make([]string, 0)
			}
			result[j] = append(result[j], string(row))
		}
		if result[rowCount] == nil {
			result[rowCount] = make([]string, 0)
		}
		result[rowCount] = append(result[rowCount], colName)
	}

	flatResult := make([]string, table.Header.CellCount+1)

	for i, row := range result {
		flatResult[i] = strings.Join(row, "|")
	}

	if stmt.Where == nil {
		return flatResult[:rowCount], nil
	}

	return EvaluateWhere(result, stmt)
}

func EvaluateFunction(result []string, stmt *SelectStatement) ([]string, error) {
	switch stmt.Exprs.Function.Name {
	case "COUNT":
		return []string{strconv.Itoa(len(result))}, nil
	default:
		return nil, errors.New("function not yet implemented")
	}
}

func EvaluateWhere(result [][]string, stmt *SelectStatement) ([]string, error) {
	if len(stmt.Where.Exprs.Args) != 3 {
		return nil, errors.New("malformed WHERE statement")
	}
	left := stmt.Where.Exprs.Args[0]
	right := stmt.Where.Exprs.Args[2]

	switch stmt.Where.Exprs.Args[1] {
	case "=":
		return FilterEqual(left, right, result)
	default:
		return nil, errors.New("WHERE operator not yet implemented")
	}
}

func FilterEqual(left, right string, result [][]string) ([]string, error) {
	right = strings.Trim(right, "'")
	colNames := result[len(result)-1]
	result = result[:len(result)-1]

	var colIdx int
	for i, colName := range colNames {
		if colName == left {
			colIdx = i
			break
		}
	}

	filteredResult := [][]string{}
	for _, row := range result {
		if strings.ToLower(row[colIdx]) == right {
			filteredResult = append(filteredResult, row)
		}
	}

	flatResult := make([]string, len(filteredResult))

	for i, row := range filteredResult {
		flatResult[i] = strings.Join(row, "|")
	}

	return flatResult, nil
}
