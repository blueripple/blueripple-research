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
    module Stan.ModelBuilder.TypedExpressions.Program,
) where

import Data.Array ((!), (//))
import qualified Data.Array as Array
import qualified Data.List.NonEmpty as NE
import Relude.Extra
import qualified Stan.ModelBuilder.BuilderTypes as SBT
import qualified Stan.ModelBuilder.TypedExpressions.Statements as TB

-- the StanBlock type has more sections so we can selectively add and remove things which are for
-- only generated quantitied or the generation of log-likelihoods
newtype StanProgram = StanProgram {unStanProgram :: Array.Array SBT.StanBlock [TB.UStmt]}

programToStmt :: SBT.GeneratedQuantities -> StanProgram -> TB.UStmt
programToStmt gq p = TB.SContext Nothing $ fullProgramStmt
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
    functionsStmtM = let x = stmtsArray ! SBT.SBFunctions in if null x then Nothing else Just (TB.SBlock TB.FunctionsStmts x)
    dataStmt =
        let d = stmtsArray ! SBT.SBData
            gqd = stmtsArray ! SBT.SBDataGQ
         in TB.SBlock TB.DataStmts (d ++ if gq /= SBT.NoGQ then gqd else [])
    tDataStmtM = let x = stmtsArray ! SBT.SBTransformedData in if null x then Nothing else Just (TB.SBlock TB.TDataStmts x)
    paramsStmt = TB.SBlock TB.ParametersStmts $ stmtsArray ! SBT.SBParameters
    tParamsStmtM = let x = stmtsArray ! SBT.SBTransformedParameters in if null x then Nothing else Just (TB.SBlock TB.TParametersStmts x)
    modelStmt =
        let ms = stmtsArray ! SBT.SBModel
            gqms = stmtsArray ! SBT.SBGeneratedQuantities
         in TB.SBlock TB.ModelStmts $ ms ++ if gq /= SBT.NoGQ then gqms else []
    gqStmtM =
        let gqs = stmtsArray ! SBT.SBGeneratedQuantities
            lls = stmtsArray ! SBT.SBLogLikelihood
         in case gq of
                SBT.NoGQ -> Nothing
                SBT.NoLL -> Just $ TB.SBlock TB.GeneratedQuantitiesStmts gqs
                SBT.OnlyLL -> Just $ TB.SBlock TB.GeneratedQuantitiesStmts lls
                SBT.All -> Just $ TB.SBlock TB.GeneratedQuantitiesStmts $ gqs ++ lls


addToBlock :: SBT.StanBlock -> TB.UStmt -> Either Text (StanProgram -> StanProgram)
addToBlock sb s = do
  let f sp = StanProgram $ unStanProgram sp // [(sb, unStanProgram sp ! sb ++ [s])]
  if sb `elem` [SBT.SBData, SBT.SBDataGQ, SBT.SBParameters]
    then case s of
           TB.SDeclare {} -> Right f
           TB.SComment {} -> Right f
           _ -> Left $ "Statement other than declaration or comment in data or parameters block."
    else Right f
