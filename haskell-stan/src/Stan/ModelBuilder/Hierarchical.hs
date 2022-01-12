{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeFamilies #-}

module Stan.ModelBuilder.Hierarchical where

import Prelude hiding (All)
import qualified Stan.ModelBuilder.Distributions as SD
import qualified Stan.ModelBuilder as SB

import qualified Control.Foldl as FL
import qualified Data.Map as Map
import qualified Data.Set as Set
import qualified Data.Vector.Unboxed as V
--import qualified Data.Dependent.HashMap as DHash
--import qualified Data.Dependent.Sum as DSum
--import qualified Stan.ModelBuilder.SumToZero as STZ
--import Stan.ModelBuilder.SumToZero (SumToZero(..))

--data NEGroupInfo r k = HGroupInfo (r -> V.Vector Double)
--newtype NEGroups r = HGroups DHash.DHashMap SB.GroupTypeTag

combineRowFuncs :: Foldable f => f (Int, r -> V.Vector Double) -> (Int, r -> V.Vector Double)
combineRowFuncs rFuncs =
  let nF = FL.premap fst FL.sum
      fF = (\r -> V.concat . fmap ($r)) <$> FL.premap snd FL.list
  in FL.fold ((,) <$> nF <*> fF) rFuncs

groupRowFunc :: (r -> k) -> (k -> Int) -> Int -> (Int, r -> V.Vector Double)
groupRowFunc rToKey keyToIndex numKeys = if numKeys == 2 then binary else nonBinary
  where


designMatrix :: SB.RowTypeTag r -> Text -> Int -> (r -> V.Vector Double) -> SB.StanBuilderM md gq ()
designMatrix rtt name rowFunction = do
