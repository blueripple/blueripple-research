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
  let ordered = snd <$> sortWith fst indexed
  return $ M.sunfoldrExactN (M.Sz ni) (\l -> (L.head l, L.tail l)) ordered


parse2D ::  Text -> Map String CS.StanStatistic -> Either T.Text (M.Array M.DL M.Ix2 CS.StanStatistic)
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
  let ordered = snd <$> sortWith (Parameters.tupleToIndex @Parameters.D2 1 (ni, nj) . fst) indexed
  return $ M.unfoldlS_ (M.Sz2 ni nj) (\l -> (L.tail l, L.head l)) ordered

parse3D ::  Text -> Map String CS.StanStatistic -> Either T.Text (M.Array M.DL M.Ix3 CS.StanStatistic)
parse3D name m = do
  let parseOneE (t, s) = do
        inds <- Parameters.parseIndex @Parameters.D3 (toText t)
        return (inds, s)
  indexed <- traverse parseOneE $ Map.toList $ onlyNamed name m
  let fldNI = FL.premap ((\(i, _, _) -> i) . fst) FL.maximum
      fldNJ = FL.premap ((\(_, j, _) -> j) . fst) FL.maximum
      fldNK = FL.premap ((\(_, _, k) -> k) . fst) FL.maximum
      f ma mb mc = do
        a <- ma
        b <- mb
        c <- mc
        return (a, b, c)
      fldIndices = f <$> fldNI <*> fldNJ <*> fldNK
  (ni, nj, nk) <- maybeToRight ("No parameters with name " <> name <> " in parse3D?") $ FL.fold fldIndices indexed
  let ordered = snd <$> sortWith (Parameters.tupleToIndex @Parameters.D3 1 (ni, nj, nk) . fst) indexed
  return $ M.unfoldlS_ (M.Sz3 ni nj nk) (\l -> (L.tail l, L.head l)) ordered

parse4D ::  Text -> Map String CS.StanStatistic -> Either T.Text (M.Array M.DL M.Ix4 CS.StanStatistic)
parse4D name m = do
  let parseOneE (t, s) = do
        inds <- Parameters.parseIndex @Parameters.D4 (toText t)
        return (inds, s)
  indexed <- traverse parseOneE $ Map.toList $ onlyNamed name m
  let fldNI = FL.premap ((\(i, _, _, _) -> i) . fst) FL.maximum
      fldNJ = FL.premap ((\(_, j, _, _) -> j) . fst) FL.maximum
      fldNK = FL.premap ((\(_, _, k, _) -> k) . fst) FL.maximum
      fldNL = FL.premap ((\(_, _, _, l) -> l) . fst) FL.maximum
      f ma mb mc md = do
        a <- ma
        b <- mb
        c <- mc
        d <- md
        return (a, b, c, d)
      fldIndices = f <$> fldNI <*> fldNJ <*> fldNK <*> fldNL
  (ni, nj, nk, nl) <- maybeToRight ("No parameters with name " <> name <> " in parse4D?") $ FL.fold fldIndices indexed
  let ordered = snd <$> sortWith (Parameters.tupleToIndex @Parameters.D4 1 (ni, nj, nk, nl) . fst) indexed
  return $ M.unfoldlS_ (M.Sz4 ni nj nk nl) (\l -> (L.tail l, L.head l)) ordered
