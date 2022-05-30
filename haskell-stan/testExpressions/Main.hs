{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
module Main where

import Stan.ModelBuilder.TypedExpressions.Types
import Stan.ModelBuilder.TypedExpressions.Indexing
import Stan.ModelBuilder.TypedExpressions.Operations
import Stan.ModelBuilder.TypedExpressions.Expressions
import Stan.ModelBuilder.TypedExpressions.Evaluate
import Stan.ModelBuilder.TypedExpressions.Recursion
import Stan.ModelBuilder.TypedExpressions.Format
import Stan.ModelBuilder.Expressions (StanExprF(IndexF))
import Stan.ModelBuilder.TypedExpressions.Statements (IndexLookupCtxt(IndexLookupCtxt))
import qualified Prettyprinter.Render.Text as PP


writeExprCode :: IndexLookupCtxt -> UExpr t -> IO ()
writeExprCode ctxt0 ue = case flip evalStateT ctxt0 $ doLookups ue of
    Left txt -> putTextLn $ "doLookups failed with message: " <> txt
    Right le -> do
      putTextLn "doLookups succeeded"
      let c = unK $ exprToCode le
      PP.putDoc c
      putTextLn ""


main :: IO ()
main = do
  -- build some expressions
  let
    plus = binaryOpE SAdd
    ue1 = namedE "x" SInt `plus` namedE "y" SInt
    ctxt0 = IndexLookupCtxt mempty mempty
  writeExprCode ctxt0 ue1
  let ue2 = sliceE s0 (namedIndexE "KIndex") (namedE "v" SCVec) `plus` (namedE "w" SInt)
      ctxt1 = IndexLookupCtxt mempty (one $ ("KIndex", lNamedE "k" SInt))
  writeExprCode ctxt0 ue2
  writeExprCode ctxt1 ue2
  let ue3 = sliceE s0 (namedE "c" SInt) $ sliceE s0 (namedE "r" SInt) (namedE "M" SMat)
  writeExprCode ctxt0 ue3
