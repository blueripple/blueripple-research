{-# LANGUAGE DataKinds #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wno-unticked-promoted-constructors #-}
module Main where

import Stan.ModelBuilder.TypedExpressions.Types
import Stan.ModelBuilder.TypedExpressions.Indexing
import Stan.ModelBuilder.TypedExpressions.Operations
import Stan.ModelBuilder.TypedExpressions.Expressions
import Stan.ModelBuilder.TypedExpressions.Evaluate
import Stan.ModelBuilder.TypedExpressions.Recursion
import Stan.ModelBuilder.TypedExpressions.Format
import Stan.ModelBuilder.TypedExpressions.Statements
import Stan.ModelBuilder.TypedExpressions.Functions
import Stan.ModelBuilder.Expressions (IndexKey)

import qualified Prettyprinter.Render.Text as PP

import qualified Data.Map as Map

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


declare :: Text -> StanType t -> Vec (DeclDimension t) (UExpr EInt) -> UStmt
declare vn vt iDecls = SDeclare vn vt (DeclIndexVecF iDecls)

declareAndAssign :: Text -> StanType t -> Vec (DeclDimension t) (UExpr EInt) -> UExpr t -> UStmt
declareAndAssign vn vt iDecls = SDeclAssign vn vt (DeclIndexVecF iDecls)

addToTarget :: UExpr EReal -> UStmt
addToTarget = STarget

assign :: UExpr t -> UExpr t -> UStmt
assign = SAssign

sample :: UExpr t -> Distribution t args -> ArgList UExpr args -> UStmt
sample = SSample

data ForType t = SpecificNumbered (UExpr EInt) (UExpr EInt)
               | IndexedLoop IndexKey
               | SpecificIn (UExpr t)
               | IndexedIn IndexKey (UExpr t)

for :: Text -> ForType t -> NonEmpty UStmt -> UStmt
for loopCounter ft body = case ft of
  SpecificNumbered se' ee' -> SFor loopCounter se' ee' body
  IndexedLoop ik -> SFor loopCounter (intE 1) (namedSizeE ik) $ bodyWithLoopCounterContext ik
  SpecificIn e -> SForEach loopCounter e body
  IndexedIn ik e -> SForEach loopCounter e $ bodyWithLoopCounterContext ik
  where
    bodyWithLoopCounterContext ik = SContext (Just $ insertUseBinding ik (lNamedE loopCounter SInt)) body :| []

ifThen :: UExpr EBool -> UStmt -> UStmt -> UStmt
ifThen ce sTrue = SIfElse $ (ce, sTrue) :| []

ifThenElse :: NonEmpty (UExpr EBool, UStmt) -> UStmt -> UStmt
ifThenElse = SIfElse

while :: UExpr EBool -> NonEmpty UStmt -> UStmt
while = SWhile

function :: Function rt args -> ArgList (FuncArg Text) args -> (ArgList UExpr args -> (NonEmpty UStmt, UExpr rt)) -> UStmt
function fd argNames bodyF = SFunction fd argNames bodyS ret
  where
    argTypes = argTypesToArgListOfTypes $ functionArgTypes fd
    argExprs = zipArgListsWith (namedE . funcArgName) argNames argTypes
    (bodyS, ret) = bodyF argExprs

insertUseBinding :: IndexKey -> LExpr EInt -> IndexLookupCtxt -> IndexLookupCtxt
insertUseBinding k ie (IndexLookupCtxt a b) = IndexLookupCtxt a (Map.insert k ie b)

insertSizeBinding :: IndexKey -> LExpr EInt -> IndexLookupCtxt -> IndexLookupCtxt
insertSizeBinding k ie (IndexLookupCtxt a b) = IndexLookupCtxt (Map.insert k ie a) b

main :: IO ()
main = do
  -- build some expressions
  let
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
  writeStmtCode ctxt0 st1
  let st2 = assign x (x `plus` (y `plus` vAtk))
  writeStmtCode ctxt0 st2
  writeStmtCode ctxt1 st2
  writeStmtCode ctxt0 $ SContext (Just $ insertUseBinding "KIndex" lk) (one st2)
  let stDeclare1 = declare "M" StanMatrix (n ::: l ::: VNil)
      nStates = namedSizeE "States"
      nPredictors = namedSizeE "Predictors"

      stDeclare2 = declare "A" (StanArray s2 StanMatrix) (n ::: l ::: nStates ::: nPredictors ::: VNil)
  writeStmtCode ctxt1 stDeclare1
  writeStmtCode ctxt0 $ SContext (Just $ insertSizeBinding "Predictors" predictorsLE) (one stDeclare2)
  writeStmtCode ctxt0 $ SContext (Just $ insertSizeBinding "States" statesLE . insertSizeBinding "Predictors" predictorsLE) (one stDeclare2)
  let stDeclAssign1 = declareAndAssign "M" StanMatrix (n ::: l ::: VNil) (namedE "q" SMat)
  writeStmtCode ctxt0 stDeclAssign1
  let stmtTarget1 = addToTarget ue2
  writeStmtCode ctxt1 stmtTarget1
  let normalDistVec = Distribution "normal" SCVec (SCVec ::> (SCVec ::> ArgTypeNil))
      stmtSample = sample v normalDistVec (namedE "m" SCVec :> (namedE "sd" SCVec :> ArgNil))
  writeStmtCode ctxt1 stmtSample
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
  let
    eq = boolOpE SEq
    stmtIf1 = ifThen (l `eq` n) st1 st2
  writeStmtCode ctxt1 stmtIf1
  let stmtWhile = while (l `eq` n) (st1 :| [st2, SBreak])
  writeStmtCode ctxt1 stmtWhile
  let
    euclideanDistance :: Function EReal [ECVec, ECVec, EArray N1 EInt]
    euclideanDistance = Function "eDist" SReal (SCVec ::> SCVec ::> SArray s1 SInt ::> ArgTypeNil)
    eDistArgList = Arg "x1" :> Arg "x2" :> DataArg "m" :> ArgNil
    eDistBody :: ArgList UExpr [ECVec, ECVec, EArray N1 EInt] -> (NonEmpty UStmt, UExpr EReal)
    eDistBody (x1 :> x2 :> _ :> ArgNil) = (one $ rv `assign` (tr (x1 `eMinus` x2) `times` (x1 `eMinus` x2)), rv)
      where rv = namedE "r" SReal
    funcStmt = function euclideanDistance eDistArgList eDistBody
  writeStmtCode ctxt0 funcStmt
  writeStmtCode ctxt0 $ SPrint (stringE "example" :> l :> ArgNil)
  writeStmtCode ctxt0 $ SReject (m :> stringE "or" :> r :> ArgNil)
