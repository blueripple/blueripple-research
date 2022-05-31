{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE DataKinds #-}
module Main where

import Stan.ModelBuilder.TypedExpressions.Types
import Stan.ModelBuilder.TypedExpressions.Indexing
import Stan.ModelBuilder.TypedExpressions.Operations
import Stan.ModelBuilder.TypedExpressions.Expressions
import Stan.ModelBuilder.TypedExpressions.Evaluate
import Stan.ModelBuilder.TypedExpressions.Recursion
import Stan.ModelBuilder.TypedExpressions.Format
--import Stan.ModelBuilder.Expressions (StanExprF(IndexF))
import Stan.ModelBuilder.TypedExpressions.Statements
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

insertUseBinding :: IndexKey -> LExpr EInt -> IndexLookupCtxt -> IndexLookupCtxt
insertUseBinding k ie (IndexLookupCtxt a b) = IndexLookupCtxt a (Map.insert k ie b)

main :: IO ()
main = do
  -- build some expressions
  let
    plus = binaryOpE SAdd
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
  writeExprCode ctxt0 ue2
  writeExprCode ctxt1 ue2
  let
    m = namedE "M" SMat
    r = namedE "r" SInt
    c = namedE "c" SInt
    ue3 = sliceE s0 c $ sliceE s0 r m
  writeExprCode ctxt0 ue3
  let st1 = SAssign ue1 ue1
  writeStmtCode ctxt0 st1
  let st2 = SAssign x (x `plus` (y `plus` vAtk))
  writeStmtCode ctxt0 st2
  writeStmtCode ctxt1 st2
  writeStmtCode ctxt0 $ SContext (Just $ insertUseBinding "KIndex" lk) [st2]
