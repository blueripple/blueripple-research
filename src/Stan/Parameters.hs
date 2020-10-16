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
module Stan.Parameters where

import qualified CmdStan.Types as CS

import qualified Control.Foldl as FL
import qualified Data.List as L
import qualified Data.Map as M
import qualified Data.Vector as V
import qualified Data.Text as T
import qualified Text.Read as R

--import qualified GHC.TypeNats as Nat

data Dim = D0 | D1 | D2 | D3 | D4

type family NTuple (n :: Dim) where
  NTuple D0 = ()
  NTuple D1 = Int
  NTuple D2 = (Int, Int)
  NTuple D3 = (Int, Int, Int)
  NTuple D4 = (Int, Int, Int, Int)

class IndexFunction (n :: Dim) where
  tupleToIndex :: Int -> NTuple n -> NTuple n -> Int
  indexToTuple :: Int -> NTuple n -> Int -> NTuple n

instance IndexFunction D0 where
  tupleToIndex offset _ _ = 0
  indexToTuple _ _ _ = ()
  
instance IndexFunction D1 where
  tupleToIndex offset _ i = (i - offset)
  indexToTuple offset _ i = (i + offset)

instance IndexFunction D2 where
  tupleToIndex offset (_, nj) (i, j) = (i - offset) * nj + (j - offset)  
  indexToTuple offset (_, nj) n = let j = n `mod` nj in (((n - j) `div` nj) + offset, j + offset)

instance IndexFunction D3 where
  tupleToIndex offset (ni, nj, nk) (i, j, k) =  tupleToIndex @D2 offset (ni, nj) (i, j) * nk + (k - offset)
  indexToTuple offset (ni, nj, nk) n =
    let k = (n `mod` nk)
        m = (n - k) `div` nk
        (i, j) = indexToTuple @D2 offset (ni, nj) m
    in (i, j, k + offset)

instance IndexFunction D4 where
  tupleToIndex offset (ni, nj, nk, nl) (i, j, k, l) = tupleToIndex @D3 offset (ni, nj, nk) (i, j, k) * nl + (l - offset)
  indexToTuple offset (ni, nj, nk, nl) n =
    let l = n `mod` nl
        m = (n - l) `div` nl
        (i, j, k) = indexToTuple @D3 offset (ni, nj, nk) m
    in (i, j, k, l + offset)

data ParameterStatistics (dim :: Dim) a where
  ParameterStatistics :: Int -> NTuple dim -> V.Vector a -> ParameterStatistics dim a

deriving instance (Show (NTuple dim), Show a) => Show (ParameterStatistics dim a)

instance Functor (ParameterStatistics dim) where
  fmap f (ParameterStatistics o nt v) = ParameterStatistics o nt (fmap f v)

getDims :: ParameterStatistics dim a -> NTuple dim
getDims (ParameterStatistics _ x _) = x
  
getIndexed :: forall dim a. IndexFunction dim => ParameterStatistics dim a -> NTuple dim -> a
getIndexed (ParameterStatistics offset dims vec) index = vec V.! tupleToIndex @dim offset dims index 

getScalar :: ParameterStatistics D0 a -> a
getScalar p@(ParameterStatistics offset _ v) = getIndexed p ()

-- parse from stansummary
class ParseIndex (n :: Dim) where
  parseIndex :: T.Text -> Either T.Text (NTuple n)

instance ParseIndex D0 where
  parseIndex t = Right $ ()

instance ParseIndex D1 where
  parseIndex t = fmap (\[i] -> i) $ parseIndex' 1 t

instance ParseIndex D2 where
  parseIndex t = fmap (\[i, j] -> (i, j)) $ parseIndex' 2 t

instance ParseIndex D3 where
  parseIndex t = fmap (\[i, j, k] -> (i, j, k)) $ parseIndex' 3 t

instance ParseIndex D4 where  
  parseIndex t = fmap (\[i, j, k, l] -> (i, j, k, l)) $ parseIndex' 4 t 

parseIndex' :: Int -> T.Text -> Either T.Text [Int]
parseIndex' n t = do
  let (_ , indices') = T.breakOn "[" t
      indicesL = T.splitOn "," $ T.drop 1 . T.dropEnd 1 $ indices'
--      indsT = fmap (T.dropEnd 1) indsT' -- drop the "]"
  inds <- traverse (either (Left . (<> ": " <> (T.pack $ show indicesL)) . T.pack) Right . R.readEither . T.unpack) indicesL
  _ <- if length inds == n
       then Right ()
       else Left ("Expecting index with " <> (T.pack $ show n) <> " components, but found " <> (T.pack . show $ length inds) <> ".")
  return inds

{-
parseIndex' :: Int -> T.Text -> Either T.Text [Int]
parseIndex' n t = do
  let (_ : indsT') = T.splitOn "[" t
      indsT = fmap (T.dropEnd 1) indsT' -- drop the "]"
  inds <- traverse (either (Left . (<> ": " <> (T.pack $ show indsT)) . T.pack) Right . R.readEither . T.unpack) indsT
  _ <- if length inds == n
       then Right ()
       else Left ("Expecting index with " <> (T.pack $ show n) <> " components, but found " <> (T.pack . show $ length inds) <> ".")
  return inds
-}

paramsByName :: T.Text -> M.Map String CS.StanStatistic -> [(String, CS.StanStatistic)]
paramsByName name = M.toList . M.filterWithKey (\k _ -> T.isPrefixOf name (T.pack k))

parseScalar :: T.Text -> M.Map String CS.StanStatistic -> Either T.Text (ParameterStatistics D0 CS.StanStatistic)
parseScalar name = maybe (Left $ "Failed to find scaler \"" <> name <> "\" in parameters") Right
                   . fmap (\s -> ParameterStatistics 1 () (V.singleton s)) . M.lookup (T.unpack name)

-- 1D: Stan is 1 indexed 
parse1D :: T.Text -> M.Map String CS.StanStatistic -> Either T.Text (ParameterStatistics D1 CS.StanStatistic)
parse1D name m = do
  let parseOneE (t, s) = do
        inds <- parseIndex @D1 (T.pack t)
        return (inds, s)
  indexed <- traverse parseOneE $ paramsByName name m 
  ni <- maybe (Left $ "No parameters with name " <> name <> " in parse1D?") Right $ FL.fold (FL.premap fst FL.maximum) indexed
  let ordered = fmap snd $ L.sortOn (tupleToIndex @D1 1 (ni - 1) . fst) indexed
  return $ ParameterStatistics 1 ni (V.fromList ordered)

parse2D :: T.Text -> M.Map String CS.StanStatistic -> Either T.Text (ParameterStatistics D2 CS.StanStatistic)
parse2D name m = do
  let parseOneE (t, s) = do
        inds <- parseIndex @D2 (T.pack t)
        return (inds, s)
  indexed <- traverse parseOneE $ paramsByName name m
  let getI (i, _) = i
      getJ (_, j) = j
  ni <- maybe (Left $ "No parameters with name " <> name <> " in parse2D?") Right $ FL.fold (FL.premap (getI . fst) FL.maximum) indexed
  nj <- maybe (Left $ "No parameters with name " <> name <> " in parse2D?") Right $ FL.fold (FL.premap (getJ . fst) FL.maximum) indexed
  let ordered = fmap snd $ L.sortOn (tupleToIndex @D2 1 (ni, nj) . fst) indexed
  return $ ParameterStatistics 1 (ni, nj) (V.fromList ordered)


parse3D :: T.Text -> M.Map String CS.StanStatistic -> Either T.Text (ParameterStatistics D3 CS.StanStatistic)
parse3D name m = do
  let parseOneE (t, s) = do
        inds <- parseIndex @D3 (T.pack t)
        return (inds, s)
  indexed <- traverse parseOneE $ paramsByName name m
  let getI (i, _, _) = i
      getJ (_, j, _) = j
      getK (_, _, k) = k
  ni <- maybe (Left $ "No parameters with name " <> name <> " in parse3D?") Right $ FL.fold (FL.premap (getI . fst) FL.maximum) indexed
  nj <- maybe (Left $ "No parameters with name " <> name <> " in parse3D?") Right $ FL.fold (FL.premap (getJ . fst) FL.maximum) indexed
  nk <- maybe (Left $ "No parameters with name " <> name <> " in parse3D?") Right $ FL.fold (FL.premap (getK . fst) FL.maximum) indexed
  let ordered = fmap snd $ L.sortOn (tupleToIndex @D3 1 (ni, nj, nk) . fst) indexed
  return $ ParameterStatistics 1 (ni, nj, nk) (V.fromList ordered)


parse4D :: T.Text -> M.Map String CS.StanStatistic -> Either T.Text (ParameterStatistics D4 CS.StanStatistic)
parse4D name m = do
  let parseOneE (t, s) = do
        inds <- parseIndex @D4 (T.pack t)
        return (inds, s)
  indexed <- traverse parseOneE $ paramsByName name m
  let getI (i, _, _, _) = i
      getJ (_, j, _, _) = j
      getK (_, _, k, _) = k
      getL (_, _, _, l) = l
  ni <- maybe (Left $ "No parameters with name " <> name <> " in parse4D?") Right $ FL.fold (FL.premap (getI . fst) FL.maximum) indexed
  nj <- maybe (Left $ "No parameters with name " <> name <> " in parse4D?") Right $ FL.fold (FL.premap (getJ . fst) FL.maximum) indexed
  nk <- maybe (Left $ "No parameters with name " <> name <> " in parse4D?") Right $ FL.fold (FL.premap (getK . fst) FL.maximum) indexed
  nl <- maybe (Left $ "No parameters with name " <> name <> " in parse4D?") Right $ FL.fold (FL.premap (getL . fst) FL.maximum) indexed
  let ordered = fmap snd $ L.sortOn (tupleToIndex @D4 1 (ni, nj, nk, nl) . fst) indexed
  return $ ParameterStatistics 1 (ni, nj, nk, nl) (V.fromList ordered)


--parameterStatistics :: ParameterStatistics a -> NTuple
