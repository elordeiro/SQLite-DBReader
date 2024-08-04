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
}

type Expr struct {
	Keyword  string
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

func ReadBeforeString(input string, delim string) (string, string) {
	var delimIdx int
	if delim == "END" {
		delimIdx = len(input)
	} else {
		delimIdx = strings.Index(input, delim)
	}
	result := strings.Trim(input[:delimIdx], " ")
	input = strings.Trim(input[delimIdx:], " ")
	return result, input
}

func ReadIncludingString(input string, delim string) (string, string) {
	result, input := ReadBeforeString(input, delim)
	result = result + input[:len(delim)]
	input = input[len(delim):]
	return result, input
}

func ParseSelectStatement(input string) (*SelectStatement, error) {
	SELECT, input := ReadIncludingString(input, "select")

	if SELECT != "select" {
		return nil, errors.New("expected SELECT")
	}

	exprs, err := ParseExpr(input)
	if err != nil {
		return nil, err
	}

	_, input = ReadBeforeString(input, "from")
	FROM, input := ReadIncludingString(input, "from")

	if FROM != "from" {
		return nil, errors.New("expected select")
	}

	from, err := ParseFrom(input)
	if err != nil {
		return nil, err
	}

	return &SelectStatement{
		Exprs: exprs,
		From:  from,
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
				Keyword: insideExpr,
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
		Keyword: expr,
	}, nil
}

func ParseFrom(input string) (*From, error) {
	keyword, _ := ReadBeforeString(input, "END")

	return &From{
		Exprs: &Expr{
			Keyword: strings.Trim(string(keyword), " "),
		},
	}, nil

}

func (page *Page) HandleCommand(input string) ([]string, error) {
	stmt, err := ParseSelectStatement(strings.ToLower(input))
	if err != nil {
		return nil, err
	}
	table, err := page.GetTablebyName(stmt.From.Exprs.Keyword)
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
	if stmt.Exprs.Keyword == "*" {
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

	tableName := stmt.From.Exprs.Keyword
	cols, _ := rootPage.GetTableColumns(tableName)

	result := make([][]string, table.Header.CellCount)
	exprs := strings.Split(stmt.Exprs.Keyword, ", ")
	for _, colName := range exprs {
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
	}

	flatResult := make([]string, table.Header.CellCount)

	for i, row := range result {
		flatResult[i] = strings.Join(row, "|")
	}

	return flatResult, nil
}

func EvaluateFunction(result []string, stmt *SelectStatement) ([]string, error) {
	switch stmt.Exprs.Function.Name {
	case "COUNT":
		return []string{strconv.Itoa(len(result))}, nil
	default:
		return nil, errors.New("function not yet implemented")
	}
}
