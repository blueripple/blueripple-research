{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wno-unticked-promoted-constructors #-}
{-# LANGUAGE RankNTypes #-}
module Main where

import Prelude hiding (print)

import Stan.ModelBuilder.TypedExpressions.Types
import Stan.ModelBuilder.TypedExpressions.TypedList
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
    Left txt -> do
      putTextLn $ "doLookups failed with message: " <> txt
      case flip evalStateT ctxt0 $ doLookupsE ue of
        Left err2 -> putTextLn $ "Yikes! Cannot even make no-lookup tree: " <> err2
        Right ee -> do
          PP.putDoc $ unK $ eExprToCode ee
          putTextLn ""
    Right le -> do
      PP.putDoc $ unK $ exprToCode le
      putTextLn ""

writeStmtCode :: IndexLookupCtxt -> UStmt -> IO ()
writeStmtCode ctxt0 s = case statementToCodeE ctxt0 s of
    Left txt -> do
      putTextLn $ "doLookups failed with message: " <> txt
      case eStatementToCodeE ctxt0 s of
        Left err2 -> putTextLn $ "Yikes! Cannot even make no-lookup tree: " <> err2
        Right ec -> do
          PP.putDoc ec
          putTextLn ""
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
    lk = lNamedE "K" (SArray s1 SInt)
    ue1 = x `plus` y
    ctxt0 = IndexLookupCtxt mempty mempty
  cmnt "Expressions"
  writeExprCode ctxt0 ue1
  let
    vByK = indexE s0 kl v
    vByKatn = sliceE s0 n vByK `plus` x
    ctxt1 = insertIndexBinding "KIndex" lk ctxt0
    statesLE = lNamedE "N_States" SInt
    predictorsLE = lNamedE "K_Predictors" SInt
    ctxt2 = insertSizeBinding "States" statesLE . insertSizeBinding "Predictors" predictorsLE $ ctxt0
  cmnt "Next one should fail with useful error tree"
  writeExprCode ctxt0 vByKatn
  writeExprCode ctxt1 vByKatn
  let
    m = namedE "M" SMat
    r = namedE "r" SInt
    c = namedE "c" SInt
    a = namedE "A" (SArray s2 SMat)
  cmnt "Indexing"
  writeExprCode ctxt0 $ sliceE s0 c $ sliceE s0 r m
  let a1 = rangeIndexE s2 (Just $ intE 2) (Just $ intE 4) a
  writeExprCode ctxt0 a1
  let a2 {-:: UExpr (EArray N2 ECVec)-} = sliceE s2 (intE 3) a1
  writeExprCode ctxt0 a2
  let a4 = indexE s2 (namedE "I" (SArray s1 SInt)) a2
  writeExprCode ctxt0 a4
  cmnt "Assignments"
  cmnt "simple, no context"
  writeStmtCode ctxt0 $ assign ue1 ue1
  cmnt "missing lookup"
  writeStmtCode ctxt0 $ assign x (x `plus` (y `plus` vByKatn))
  cmnt "with context"
  writeStmtCode ctxt1 $ assign x (x `plus` (y `plus` vByKatn))
  cmnt "context added in tree"
  writeStmtCode ctxt0 $ SContext (Just $ insertIndexBinding "KIndex" lk) [assign x (x `plus` (y `plus` vByKatn))]
  let stDeclare1 = declare "M" (matrixSpec n l [])
      nStates = namedSizeE "States"
      nPredictors = namedSizeE "Predictors"

      stDeclare2 = declare "A" $ arraySpec s2 (n ::: l ::: VNil) (matrixSpec nStates nPredictors [lowerM $ realE 2])
  cmnt "Declarations"
  writeStmtCode ctxt1 stDeclare1
  writeStmtCode ctxt0 $ SContext (Just $ insertSizeBinding "Predictors" predictorsLE) [stDeclare2]
  writeStmtCode ctxt0 $ SContext (Just $ insertSizeBinding "States" statesLE . insertSizeBinding "Predictors" predictorsLE) [stDeclare2]
  let stDeclAssign1 = declareAndAssign "M" (matrixSpec l n [upperM $ realE 8]) (namedE "q" SMat)
  writeStmtCode ctxt0 stDeclAssign1
  writeStmtCode ctxt0 $ declareAndAssign "v1" (vectorSpec (intE 2) []) (vectorE [1,2])
  writeStmtCode ctxt0 $ declareAndAssign "v2" (vectorSpec (intE 2) []) (rangeIndexE s0 (Just $ intE 2) (Just $ intE 3) v)
  writeStmtCode ctxt0 $ declareAndAssign "A" (matrixSpec (intE 2) (intE 2) []) (matrixE [(2 ::: 3 ::: VNil), (4 ::: 5 ::: VNil)])
  writeStmtCode ctxt0 $ declareAndAssign "B" (arraySpec s2 (intE 2 ::: intE 2 ::: VNil) $ realSpec [lowerM $ realE 0])
    (arrayE $ NestedVec2 ((realE 2 ::: realE 3 ::: VNil) ::: (realE 4 ::: realE 5 ::: VNil) :::  VNil))
  writeStmtCode ctxt0 $ declareAndAssign "C" (arraySpec s2 (intE 2 ::: intE 2 ::: VNil) (vectorSpec (intE 2) [lowerM $ realE 0 , multiplierM $ realE 3]))
    (arrayE $ NestedVec2 ((vectorE [1,2] ::: vectorE [3,4] ::: VNil) ::: (vectorE [4,5] ::: vectorE [5, 6] ::: VNil) :::  VNil))
  cmnt "Add to target, two ways."
  let normalDistVec = Density "normal" SCVec (SCVec ::> (SCVec ::> TypeNil)) id
      stmtTarget1 = addToTarget $ densityE normalDistVec v (namedE "m" SCVec :> (namedE "sd" SCVec :> TNil))
  writeStmtCode ctxt1 stmtTarget1
  let stmtSample = sample v normalDistVec (namedE "m" SCVec :> (namedE "sd" SCVec :> TNil))
  writeStmtCode ctxt1 stmtSample
  cmnt "For loops, four ways"
  let stmtFor1 = for "k" (SpecificNumbered (intE 1) n) (\ke -> [assign x (x `plus` (y `plus` sliceE s0 ke vByK))])
  writeStmtCode ctxt1 stmtFor1
  let
    bodyF2 se = assign (sliceE s0 se $ namedE "w" SCVec) (realE 2) :| []
    stmtFor2 = for "q" (IndexedLoop "States") bodyF2
  writeStmtCode ctxt2 stmtFor2
  let stmtFor3 = for "yl" (SpecificIn $ namedE "ys" SCVec) (\ye -> [assign x (x `plus` ye)])
  writeStmtCode ctxt0 stmtFor3
  let stmtFor4 = for "q" (IndexedIn "States" $ namedE "votes" SCVec)
        $ \sie -> (assign (sliceE s0 sie $ namedE "w" SCVec) (realE 2) :| [assign x (x `plus` y), SContinue])
  writeStmtCode ctxt1 stmtFor4
  cmnt "Conditionals"
  let
    eq = boolOpE SEq
    stmtIf1 = ifThen (l `eq` n) (assign ue1 ue1) (assign x (x `plus` y))
  writeStmtCode ctxt1 stmtIf1
  cmnt "While loops"
  let stmtWhile = while (l `eq` n) (assign ue1 ue1 :| [assign x (x `plus` y), SBreak])
  writeStmtCode ctxt1 stmtWhile
  cmnt "Functions"
  let
    euclideanDistance :: Function EReal [ECVec, ECVec, EArray N1 EInt]
    euclideanDistance = Function "eDist" SReal (SCVec ::> SCVec ::> SArray s1 SInt ::> TypeNil) id
    eDistArgList = Arg "x1" :> Arg "x2" :> DataArg "m" :> TNil
    eDistBody :: ExprList [ECVec, ECVec, EArray N1 EInt] -> (NonEmpty UStmt, UExpr EReal)
    eDistBody (x1 :> x2 :> _ :> TNil) = (one $ rv `assign` (tr (x1 `eMinus` x2) `times` (x1 `eMinus` x2)), rv)
      where rv = namedE "r" SReal
    funcStmt = function euclideanDistance eDistArgList eDistBody
  writeStmtCode ctxt0 funcStmt
  cmnt "print/reject"
  writeStmtCode ctxt0 $ print (stringE "example" :> l :> TNil)
  writeStmtCode ctxt0 $ reject (m :> stringE "or" :> r :> TNil)
  writeStmtCode ctxt0 $ comment ("Multiline comments" :| ["are formatted differently!"])
