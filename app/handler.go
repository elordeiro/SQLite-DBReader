package main

import (
	"bufio"
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

func ParseSelectStatement(input string) (*SelectStatement, error) {
	buf := bufio.NewReader(strings.NewReader(input))
	SELECT, err := buf.ReadBytes(' ')
	if err != nil {
		return nil, err
	}
	if string(SELECT) != "select " {
		return nil, errors.New("expected SELECT")
	}

	exprs, err := ParseExpr(buf)
	if err != nil {
		return nil, err
	}

	FROM, err := buf.ReadBytes(' ')
	if err != nil {
		return nil, err
	}

	if string(FROM) != "from " {
		return nil, errors.New("expected select")
	}

	from, err := ParseFrom(buf)
	if err != nil {
		return nil, err
	}

	return &SelectStatement{
		Exprs: exprs,
		From:  from,
	}, nil
}

func ParseExpr(buf *bufio.Reader) (*Expr, error) {
	exprBytes, err := buf.ReadBytes(' ')
	if err != nil {
		return nil, err
	}
	expr := strings.Trim(string(exprBytes), " ")
	if strings.Contains(expr, "(") {
		openParenIdx := strings.Index(expr, "(")
		function := expr[:openParenIdx]
		switch function {
		case "count":
			insideExpr := expr[openParenIdx+1 : strings.Index(expr, ")")]
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

func ParseFrom(buf *bufio.Reader) (*From, error) {
	keyword, err := buf.ReadBytes(' ')
	if err != nil && err.Error() != "EOF" {
		return nil, err
	}

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
	var colNum int
	colName := stmt.Exprs.Keyword
	for i, col := range cols {
		if strings.Contains(col, colName) {
			colNum = i
			break
		}
	}

	// for i, col := range cols {
	// 	if i >= colNum {
	// 		break
	// 	}
	// 	if strings.Contains(col, "autoincrement") {
	// 		colNum--
	// 		break
	// 	}
	// }

	var result []string
	for _, cell := range table.Cells {
		row := cell.Record.Keys[colNum]
		result = append(result, string(row))
	}
	return result, nil
}

func EvaluateFunction(result []string, stmt *SelectStatement) ([]string, error) {
	switch stmt.Exprs.Function.Name {
	case "COUNT":
		return []string{strconv.Itoa(len(result))}, nil
	default:
		return nil, errors.New("function not yet implemented")
	}
}
