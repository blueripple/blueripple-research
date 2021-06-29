{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
module Stan.Parameters.Massiv where

import qualified CmdStan.Types as CS
import qualified Stan.Parameters as Parameters

import qualified Control.Foldl as FL
import qualified Data.List as L
import qualified Data.Map as Map
import qualified Data.Massiv.Array as M
--import qualified Data.Vector as V
import qualified Data.Text as T
--import qualified Text.Read as R

{-
Simpler interface for parameter parsing.  Parse directly into Massiv Arrays of StanStatistic.
-}

parseScalar ::  Text -> Map String CS.StanStatistic -> Either Text CS.StanStatistic
parseScalar  name = maybeToRight ("Failed to find scalar \"" <> name <> "\" in parameters")
                   . Map.lookup (toString name)


onlyNamed :: Text -> Map String CS.StanStatistic -> Map String CS.StanStatistic
onlyNamed name = Map.filterWithKey (\k _ -> name == fst (T.break (== '[') (toText k)))

parse1D ::  Text -> Map String CS.StanStatistic -> Either T.Text (M.Vector M.DS CS.StanStatistic)
parse1D name m = do
  let parseOneE (t, s) = do
        inds <- Parameters.parseIndex @Parameters.D1 (toText t)
        return (inds, s)
  indexed <- traverse parseOneE $ Map.toList $ onlyNamed name m
  ni <- maybeToRight ("No parameters with name " <> name <> " in parse1D?") $ FL.fold (FL.premap fst FL.maximum) indexed
  let ordered = snd <$> L.sortOn fst indexed
  return $ M.sunfoldrExactN (M.Sz ni) (\l -> (L.head l, L.tail l)) ordered

{-
parse2D ::  Text -> Map String CS.StanStatistic -> Either T.Text (M.Array M.Ix2 M.DS CS.StanStatistic)
parse2D name m = do
  let parseOneE (t, s) = do
        inds <- Parameters.parseIndex @Parameters.D2 (toText t)
        return (inds, s)
  indexed <- traverse parseOneE $ Map.toList $ onlyNamed name m
  let fldNI = FL.premap ((\(i, _) -> i) . fst) FL.maximum
      fldNJ = FL.premap ((\(_, j) -> j) . fst) FL.maximum
      f ma mb = do
        a <- ma
        b <- mb
        return (a, b)
      fldIndices = f <$> fldNI <*> fldNJ
  (ni, nj) <- maybeToRight ("No parameters with name " <> name <> " in parse2D?") $ FL.fold fldIndices indexed
  let ordered =
-}
