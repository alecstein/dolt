// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dfunctions

import (
	"errors"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

const DoltBranchFuncName = "dolt_branch"

var EmptyBranchNameErr = errors.New("error: cannot branch empty string")
var InvalidArgErr = errors.New("error: invalid usage")

type DoltBranchFunc struct {
	expression.NaryExpression
}

func NewDoltBranchFunc(args ...sql.Expression) (sql.Expression, error) {
	return &DoltBranchFunc{expression.NaryExpression{ChildExpressions: args}}, nil
}

func (d DoltBranchFunc) String() string {
	childrenStrings := make([]string, len(d.Children()))

	for i, child := range d.Children() {
		childrenStrings[i] = child.String()
	}

	return fmt.Sprintf("DOLT_BRANCH(%s)", strings.Join(childrenStrings, ","))
}

func (d DoltBranchFunc) Type() sql.Type {
	return sql.Int8
}

func (d DoltBranchFunc) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewDoltBranchFunc(children...)
}

func (d DoltBranchFunc) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	dbName := ctx.GetCurrentDatabase()

	if len(dbName) == 0 {
		return 1, fmt.Errorf("Empty database name.")
	}

	ap := cli.CreateBranchArgParser()

	args, err := getDoltArgs(ctx, row, d.Children())
	if err != nil {
		return 1, err
	}

	apr, err := ap.Parse(args)
	if err != nil {
		return 1, err
	}

	dSess := dsess.DSessFromSess(ctx.Session)
	dbData, ok := dSess.GetDbData(ctx, dbName)
	if !ok {
		return 1, fmt.Errorf("Could not load database %s", dbName)
	}

	// TODO: prevent deletion / renaming branches that have connected users that are working on them.

	switch {
	case apr.Contains(cli.CopyFlag):
		err = makeACopyOfBranch(ctx, dbData, apr)
		if err != nil {
			return 1, err
		}
	case apr.Contains(cli.MoveFlag):
		// TODO: check if user tries to rename 'main' branch
		err = renameBranch(ctx, dbData, apr)
		if err != nil {
			return 1, err
		}
	case apr.Contains(cli.DeleteFlag):
		err = deleteBranches(ctx, dbData, apr, apr.Contains(cli.ForceFlag))
		if err != nil {
			return 1, err
		}
	case apr.Contains(cli.DeleteForceFlag):
		err = deleteBranches(ctx, dbData, apr, true)
		if err != nil {
			return 1, err
		}
	default:
		// regular branch - create new branch
		if apr.NArg() != 1 {
			return 1, InvalidArgErr
		}

		branchName := apr.Arg(0)
		if len(branchName) == 0 {
			return 1, EmptyBranchNameErr
		}

		err = createNewBranch(ctx, dbData, branchName)
		if err != nil {
			return 1, err
		}
	}

	return 0, nil
}

func createNewBranch(ctx *sql.Context, dbData env.DbData, branchName string) error {
	// Check if the branch already exists.
	isBranch, err := actions.IsBranch(ctx, dbData.Ddb, branchName)
	if err != nil {
		return err
	} else if isBranch {
		return errors.New(fmt.Sprintf("fatal: A branch named '%s' already exists.", branchName))
	}

	startPt := fmt.Sprintf("head")
	return actions.CreateBranchWithStartPt(ctx, dbData, branchName, startPt, false)
}

func makeACopyOfBranch(ctx *sql.Context, dbData env.DbData, apr *argparser.ArgParseResults) error {
	if apr.NArg() != 2 {
		return InvalidArgErr
	}

	srcBr := apr.Args[0]
	if len(srcBr) == 0 {
		return EmptyBranchNameErr
	}

	destBr := apr.Args[1]
	if len(destBr) == 0 {
		return EmptyBranchNameErr
	}

	force := apr.Contains(cli.ForceFlag)
	return copyABranch(ctx, dbData, srcBr, destBr, force)
}

func copyABranch(ctx *sql.Context, dbData env.DbData, srcBr string, destBr string, force bool) error {
	err := actions.CopyBranchOnDB(ctx, dbData.Ddb, srcBr, destBr, force)
	if err != nil {
		if err == doltdb.ErrBranchNotFound {
			return errors.New(fmt.Sprintf("fatal: A branch named '%s' not found", srcBr))
		} else if err == actions.ErrAlreadyExists {
			return errors.New(fmt.Sprintf("fatal: A branch named '%s' already exists.", destBr))
		} else if err == doltdb.ErrInvBranchName {
			return errors.New(fmt.Sprintf("fatal: '%s' is not a valid branch name.", destBr))
		} else {
			return errors.New(fmt.Sprintf("fatal: Unexpected error copying branch from '%s' to '%s'", srcBr, destBr))
		}
	}

	return nil
}

func renameBranch(ctx *sql.Context, dbData env.DbData, apr *argparser.ArgParseResults) error {
	if apr.NArg() != 2 {
		return InvalidArgErr
	}

	oldBr := apr.Args[0]
	if len(oldBr) == 0 {
		return EmptyBranchNameErr
	}

	newBr := apr.Args[1]
	if len(newBr) == 0 {
		return EmptyBranchNameErr
	}

	force := apr.Contains(cli.ForceFlag)
	err := copyABranch(ctx, dbData, oldBr, newBr, force)
	if err != nil {
		return err
	}

	oldRef := ref.NewBranchRef(oldBr)
	newRef := ref.NewBranchRef(newBr)

	// TODO : need to handle checkout to different branch outside of the session
	if ref.Equals(dbData.Rsr.CWBHeadRef(), oldRef) {
		//err = dbData.Rsw.SetCWBHeadRef(ctx, ref.MarshalableRef{Ref: newRef})
		//if err != nil {
		//	return err
		//}
		return errors.New("error: Cannot rename checked out branch")
	}

	fromWSRef, err := ref.WorkingSetRefForHead(oldRef)
	if err != nil {
		if !errors.Is(err, ref.ErrWorkingSetUnsupported) {
			return err
		}
	} else {
		toWSRef, tErr := ref.WorkingSetRefForHead(newRef)
		if tErr != nil {
			return tErr
		}
		// We always force here, because the CopyBranch up above created
		// a new branch, and it will have a working set.
		err = dbData.Ddb.CopyWorkingSet(ctx, fromWSRef, toWSRef, true)
		if err != nil {
			return err
		}
	}

	err = deleteABranch(ctx, dbData, oldBr, true)
	if err != nil {
		return err
	}

	return nil
}

func deleteBranches(ctx *sql.Context, dbData env.DbData, apr *argparser.ArgParseResults, force bool) error {
	if apr.NArg() == 0 {
		return InvalidArgErr
	}

	for _, brName := range apr.Args {
		if len(brName) == 0 {
			return EmptyBranchNameErr
		}
		err := deleteABranch(ctx, dbData, brName, force)
		if err != nil {
			if err == doltdb.ErrBranchNotFound {
				return errors.New(fmt.Sprintf("fatal: A branch named '%s' not found", brName))
			} else if err == actions.ErrCOBranchDelete {
				return errors.New(fmt.Sprintf("error: Cannot delete checked out branch '%s'", brName))
			} else {
				return errors.New(fmt.Sprintf("fatal: Unexpected error deleting '%s'", brName))
			}
		}
	}

	return nil
}

func deleteABranch(ctx *sql.Context, dbData env.DbData, brName string, force bool) error {
	var remote bool
	var dref ref.DoltRef

	// TODO : add handling remote branch
	dref = ref.NewBranchRef(brName)
	if ref.Equals(dbData.Rsr.CWBHeadRef(), dref) {
		return errors.New("attempted to delete checked out branch")
	}

	hasRef, err := dbData.Ddb.HasRef(ctx, dref)

	if err != nil {
		return err
	} else if !hasRef {
		return errors.New(fmt.Sprintf("fatal: A branch named '%s' not found", brName))
	}

	if !force && !remote {
		// env.GetDefaultInitBranch(dEnv.Config) => "main"
		// TODO: get parent commit spec?
		ms, err := doltdb.NewCommitSpec("main")
		if err != nil {
			return err
		}

		init, err := dbData.Ddb.Resolve(ctx, ms, nil)
		if err != nil {
			return err
		}

		cs, err := doltdb.NewCommitSpec(dref.String())
		if err != nil {
			return err
		}

		cm, err := dbData.Ddb.Resolve(ctx, cs, nil)
		if err != nil {
			return err
		}

		isMerged, _ := init.CanFastReverseTo(ctx, cm)
		if err != nil && !errors.Is(err, doltdb.ErrUpToDate) {
			return err
		}
		if !isMerged {
			return errors.New("attempted to delete a branch that is not fully merged into its parent; use `-f` to force")
		}
	}

	wsRef, err := ref.WorkingSetRefForHead(dref)
	if err != nil {
		if !errors.Is(err, ref.ErrWorkingSetUnsupported) {
			return err
		}
	} else {
		err = dbData.Ddb.DeleteWorkingSet(ctx, wsRef)
		if err != nil {
			return err
		}
	}

	return dbData.Ddb.DeleteBranch(ctx, dref)
}
