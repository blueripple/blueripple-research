{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE UndecidableInstances #-}
{-# OPTIONS_GHC -fwarn-incomplete-patterns #-}

module Stan.ModelBuilder.TypedExpressions.Program (
    module Stan.ModelBuilder.TypedExpressions.Program
) where

import qualified Control.Foldl as FL
import Data.Array ((!), (//))
import qualified Data.Array as Array
import qualified Data.List.NonEmpty as NE
import Relude.Extra
import qualified Stan.ModelBuilder.BuilderTypes as SBT
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TE
import qualified Stan.ModelBuilder.TypedExpressions.Evaluate as TE
import qualified Stan.ModelBuilder.TypedExpressions.Recursion as TE
import qualified Stan.ModelBuilder.TypedExpressions.Format as TE

import qualified Prettyprinter.Render.Text as PP
import qualified Prettyprinter as PP


-- the StanBlock type has more sections so we can selectively add and remove things which are for
-- only generated quantitied or the generation of log-likelihoods
newtype StanProgram = StanProgram {unStanProgram :: Array.Array SBT.StanBlock [TE.UStmt]}

emptyStanProgram :: StanProgram
emptyStanProgram = StanProgram $ Array.listArray (minBound, maxBound) $ repeat []

programHasLLBlock :: StanProgram -> Bool
programHasLLBlock p = null (unStanProgram p Array.! SBT.SBGeneratedQuantities)

-- this is...precarious.  No way to check that we are using all of the array
programToStmt :: SBT.GeneratedQuantities -> StanProgram -> TE.UStmt
programToStmt gq p = TE.SContext Nothing fullProgramStmt
  where
    stmtsArray = unStanProgram p
    fullProgramStmt  =
      let (s, ss1) = let d = dataStmt in maybe (d, []) (\x -> (x, [d])) functionsStmtM
          ss2 = ss1 ++ maybe [] pure tDataStmtM
          ss3 = ss2 ++ [paramsStmt]
          ss4 = ss3 ++ maybe [] pure tParamsStmtM
          ss5 = ss4 ++ [modelStmt]
          ss6 = ss5 ++ maybe [] pure gqStmtM
      in s :| ss6
    functionsStmtM = let x = stmtsArray ! SBT.SBFunctions in if null x then Nothing else Just (TE.SBlock TE.FunctionsStmts x)
    dataStmt =
        let d = stmtsArray ! SBT.SBData
            gqd = stmtsArray ! SBT.SBDataGQ
         in TE.SBlock TE.DataStmts (d ++ if gq /= SBT.NoGQ then gqd else [])
    tDataStmtM =
      let
        x = stmtsArray ! SBT.SBTransformedData
        xGQ = stmtsArray ! SBT.SBTransformedDataGQ
      in if null x && null xGQ then Nothing else Just (TE.SBlock TE.TDataStmts $ x ++ xGQ)
    paramsStmt = TE.SBlock TE.ParametersStmts $ stmtsArray ! SBT.SBParameters
    tParamsStmtM = let x = stmtsArray ! SBT.SBTransformedParameters in if null x then Nothing else Just (TE.SBlock TE.TParametersStmts x)
    modelStmt =
        let ms = stmtsArray ! SBT.SBModel
            gqms = stmtsArray ! SBT.SBGeneratedQuantities
         in TE.SBlock TE.ModelStmts $ ms ++ if gq /= SBT.NoGQ then gqms else []
    gqStmtM =
        let gqs = stmtsArray ! SBT.SBGeneratedQuantities
            lls = stmtsArray ! SBT.SBLogLikelihood
         in case gq of
                SBT.NoGQ -> Nothing
                SBT.NoLL -> Just $ TE.SBlock TE.GeneratedQuantitiesStmts gqs
                SBT.OnlyLL -> Just $ TE.SBlock TE.GeneratedQuantitiesStmts lls
                SBT.All -> Just $ TE.SBlock TE.GeneratedQuantitiesStmts $ gqs ++ lls


-- check if the type of statement is allowed in the block then, if so, provide the modification function
-- otherwise error
addStmtToBlock' :: ([TE.UStmt] -> TE.UStmt -> [TE.UStmt]) -> SBT.StanBlock -> TE.UStmt -> Either Text (StanProgram -> StanProgram)
addStmtToBlock' addF sb s = do
  let f sp =
        let p = unStanProgram sp
        in StanProgram $ p // [(sb, p ! sb `addF` s)]
  case s of
    TE.SFunction {} -> if sb == SBT.SBFunctions
                       then Right f
                       else Left "Functions and only functions can appear in the function block."
    _ -> if sb `elem` [SBT.SBData, SBT.SBDataGQ, SBT.SBParameters]
      then case s of
             TE.SDeclare {} -> Right f
             TE.SComment {} -> Right f
             _ -> Left $ "Statement other than declaration or comment in data or parameters block: \n"
               <> (case stmtAsText s of
                     Left err -> "Error trying to render statement (" <> err <> ")"
                     Right st -> st)
      else Right f

addStmtToBlock :: SBT.StanBlock -> TE.UStmt -> Either Text (StanProgram -> StanProgram)
addStmtToBlock = addStmtToBlock' (\stmts s -> stmts ++ [s])

addStmtToBlockTop :: SBT.StanBlock -> TE.UStmt -> Either Text (StanProgram -> StanProgram)
addStmtToBlockTop = addStmtToBlock' $ flip (:)

addStmtsToBlock :: Traversable f => SBT.StanBlock -> f TE.UStmt -> Either Text (StanProgram -> StanProgram)
addStmtsToBlock b stmts = do
  fs <- traverse (addStmtToBlock b) stmts
  let g sp = foldl' (\sp f -> f sp) sp fs
  return g

addStmtsToBlockTop :: Traversable f => SBT.StanBlock -> f TE.UStmt -> Either Text (StanProgram -> StanProgram)
addStmtsToBlockTop b stmts = do
  fs <- traverse (addStmtToBlockTop b) $ reverse $ FL.fold FL.list stmts
  let g sp = foldl' (\sp f -> f sp) sp fs
  return g

programAsText :: SBT.GeneratedQuantities -> StanProgram -> Either Text Text
programAsText gq p = stmtAsText $ programToStmt gq p
{-do
  let pStmt = programToStmt gq p
  case TE.statementToCodeE TE.emptyLookupCtxt pStmt of
    Right x -> pure $ PP.renderStrict $ PP.layoutSmart PP.defaultLayoutOptions x
    Left err ->
      let msg = "Lookup error when building code from tree: " <> err <> "\n"
            <> "Tree with failed lookups between hashes follows.\n"
            <> case TE.eStatementToCodeE TE.emptyLookupCtxt pStmt of
                 Left err2 -> "Yikes! Can't build error tree: " <> err2 <> "\n"
                 Right x -> PP.renderStrict $ PP.layoutSmart PP.defaultLayoutOptions x
      in Left msg
-}
stmtAsText :: TE.UStmt -> Either Text Text
stmtAsText  stmt = case TE.statementToCodeE TE.emptyLookupCtxt stmt of
  Right x -> pure $ PP.renderStrict $ PP.layoutSmart PP.defaultLayoutOptions x
  Left err ->
    let msg = "Lookup error when building code from tree: " <> err <> "\n"
              <> "Tree with failed lookups between hashes follows.\n"
              <> case TE.eStatementToCodeE TE.emptyLookupCtxt stmt of
                   Left err2 -> "Yikes! Can't build error tree: " <> err2 <> "\n"
                   Right x -> PP.renderStrict $ PP.layoutSmart PP.defaultLayoutOptions x
    in Left msg
