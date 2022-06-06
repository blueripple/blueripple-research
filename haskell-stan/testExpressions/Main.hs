{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wno-unticked-promoted-constructors #-}
module Main where

import Prelude hiding (print)

import Stan.ModelBuilder.TypedExpressions.Types
import Stan.ModelBuilder.TypedExpressions.Indexing
import Stan.ModelBuilder.TypedExpressions.Operations
import Stan.ModelBuilder.TypedExpressions.Expressions
import Stan.ModelBuilder.TypedExpressions.Evaluate
import Stan.ModelBuilder.TypedExpressions.Recursion
import Stan.ModelBuilder.TypedExpressions.Format
import Stan.ModelBuilder.TypedExpressions.Statements
import Stan.ModelBuilder.TypedExpressions.Functions

import qualified Prettyprinter.Render.Text as PP

writeExprCode :: IndexLookupCtxt -> UExpr t -> IO ()
writeExprCode ctxt0 ue = case flip evalStateT ctxt0 $ doLookups ue of
    Left txt -> putTextLn $ "doLookups failed with message: " <> txt
    Right le -> do
      PP.putDoc $ unK $ exprToCode le
      putTextLn ""

writeStmtCode :: IndexLookupCtxt -> UStmt -> IO ()
writeStmtCode ctxt0 s = case statementToCodeE ctxt0 s of
    Left txt -> putTextLn $ "doLookups failed with message: " <> txt
    Right c -> do
      PP.putDoc c
      putTextLn ""


main :: IO ()
main = do
  -- build some expressions
  let
    cmnt t = writeStmtCode ctxt0 $ comment (one t)
    plus = binaryOpE SAdd
    eMinus = binaryOpE $ SElementWise SSubtract
    times = binaryOpE SMultiply
    tr = unaryOpE STranspose
    n = namedE "n" SInt
    l = namedE "l" SInt
    x = namedE "x" SReal
    y = namedE "y" SReal
    v = namedE "v" SCVec
    kl = namedIndexE "KIndex"
    lk = lNamedE "k" SInt
    ue1 = x `plus` y
    ctxt0 = IndexLookupCtxt mempty mempty
  cmnt "Expressions"
  writeExprCode ctxt0 ue1
  let
    vAtk = sliceE s0 kl v
    ue2 = vAtk `plus` x
    ctxt1 = insertUseBinding "KIndex" lk ctxt0
    statesLE = lNamedE "N_States" SInt
    predictorsLE = lNamedE "K_Predictors" SInt
    ctxt2 = insertSizeBinding "States" statesLE . insertSizeBinding "Predictors" predictorsLE $ ctxt0
  writeExprCode ctxt0 ue2
  writeExprCode ctxt1 ue2
  let
    m = namedE "M" SMat
    r = namedE "r" SInt
    c = namedE "c" SInt
    ue3 = sliceE s0 c $ sliceE s0 r m
  writeExprCode ctxt0 ue3
  let st1 = assign ue1 ue1
  cmnt "Assignments"
  writeStmtCode ctxt0 st1
  let st2 = assign x (x `plus` (y `plus` vAtk))
  writeStmtCode ctxt0 st2
  writeStmtCode ctxt1 st2
  writeStmtCode ctxt0 $ SContext (Just $ insertUseBinding "KIndex" lk) (one st2)
  let stDeclare1 = declare "M" (matrixSpec n l)
      nStates = namedSizeE "States"
      nPredictors = namedSizeE "Predictors"

      stDeclare2 = declare "A" $ arraySpec s2 (n ::: l ::: VNil) (matrixSpec nStates nPredictors)
  cmnt "Declarations"
  writeStmtCode ctxt1 stDeclare1
  writeStmtCode ctxt0 $ SContext (Just $ insertSizeBinding "Predictors" predictorsLE) (one stDeclare2)
  writeStmtCode ctxt0 $ SContext (Just $ insertSizeBinding "States" statesLE . insertSizeBinding "Predictors" predictorsLE) (one stDeclare2)
  let stDeclAssign1 = declareAndAssign "M" (matrixSpec l n) (namedE "q" SMat)
  writeStmtCode ctxt0 stDeclAssign1
  writeStmtCode ctxt0 $ declareAndAssign "v1" (vectorSpec $ intE 2) (vectorE [1,2])
  writeStmtCode ctxt0 $ declareAndAssign "A" (matrixSpec (intE 2) (intE 2)) (matrixE [(2 ::: 3 ::: VNil), (4 ::: 5 ::: VNil)])
  writeStmtCode ctxt0 $ declareAndAssign "B" (arraySpec s2 (intE 2 ::: intE 2 ::: VNil) realSpec)
    (arrayE $ NestedVec2 ((realE 2 ::: realE 3 ::: VNil) ::: (realE 4 ::: realE 5 ::: VNil) :::  VNil))
  writeStmtCode ctxt0 $ declareAndAssign "C" (arraySpec s2 (intE 2 ::: intE 2 ::: VNil) (vectorSpec $ intE 2))
    (arrayE $ NestedVec2 ((vectorE [1,2] ::: vectorE [3,4] ::: VNil) ::: (vectorE [4,5] ::: vectorE [5, 6] ::: VNil) :::  VNil))
  let stmtTarget1 = addToTarget ue2
  cmnt "Add to target, two ways."
  writeStmtCode ctxt1 stmtTarget1
  let normalDistVec = Distribution "normal" SCVec (SCVec ::> (SCVec ::> ArgTypeNil))
      stmtSample = sample v normalDistVec (namedE "m" SCVec :> (namedE "sd" SCVec :> ArgNil))
  writeStmtCode ctxt1 stmtSample
  cmnt "For loops, four ways"
  let stmtFor1 = for "k" (SpecificNumbered (intE 2) n) (one st2)
  writeStmtCode ctxt1 stmtFor1
  let
    stateS = assign (sliceE s0 (namedIndexE "States") $ namedE "w" SCVec) (realE 2)
    stmtFor2 = for "q" (IndexedLoop "States") (one stateS)
  writeStmtCode ctxt2 $ stmtFor2
  let stmtFor3 = for "k" (SpecificIn $ namedE "ys" SCVec) (one st2)
  writeStmtCode ctxt1 stmtFor3
  let stmtFor4 = for "q" (IndexedIn "States" $ namedE "votes" SCVec) (stateS :| [st2, SContinue])
  writeStmtCode ctxt1 stmtFor4
  cmnt "Conditionals"
  let
    eq = boolOpE SEq
    stmtIf1 = ifThen (l `eq` n) st1 st2
  writeStmtCode ctxt1 stmtIf1
  cmnt "While loops"
  let stmtWhile = while (l `eq` n) (st1 :| [st2, SBreak])
  writeStmtCode ctxt1 stmtWhile
  cmnt "Functions"
  let
    euclideanDistance :: Function EReal [ECVec, ECVec, EArray N1 EInt]
    euclideanDistance = Function "eDist" SReal (SCVec ::> SCVec ::> SArray s1 SInt ::> ArgTypeNil)
    eDistArgList = Arg "x1" :> Arg "x2" :> DataArg "m" :> ArgNil
    eDistBody :: ArgList UExpr [ECVec, ECVec, EArray N1 EInt] -> (NonEmpty UStmt, UExpr EReal)
    eDistBody (x1 :> x2 :> _ :> ArgNil) = (one $ rv `assign` (tr (x1 `eMinus` x2) `times` (x1 `eMinus` x2)), rv)
      where rv = namedE "r" SReal
    funcStmt = function euclideanDistance eDistArgList eDistBody
  writeStmtCode ctxt0 funcStmt
  cmnt "print/reject"
  writeStmtCode ctxt0 $ print (stringE "example" :> l :> ArgNil)
  writeStmtCode ctxt0 $ reject (m :> stringE "or" :> r :> ArgNil)
  writeStmtCode ctxt0 $ comment ("Multiline comments" :| ["are formatted differently!"])
