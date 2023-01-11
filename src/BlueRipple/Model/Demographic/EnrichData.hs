{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE UndecidableInstances #-}
{-# LANGUAGE UnicodeSyntax #-}

module BlueRipple.Model.Demographic.EnrichData
  (
    module BlueRipple.Model.Demographic.EnrichData
  )
where

import qualified BlueRipple.Model.Demographic.StanModels as DM

import qualified BlueRipple.Data.ACS_PUMS as PUMS
import qualified BlueRipple.Data.DemographicTypes as DT
import qualified BlueRipple.Utilities.KnitUtils as BRK
import qualified BlueRipple.Data.DataFrames as BRDF

import qualified Control.MapReduce.Simple as MR
import qualified Frames.Transform as FT
import qualified Frames.Streamly.Transform as FST
import qualified Frames.Streamly.InCore as FSI
import qualified Streamly.Prelude as Streamly

import qualified Control.Foldl as FL
import Control.Monad.Catch (throwM, Exception, MonadThrow)
import qualified Data.Map as M
import Data.Type.Equality (type (~))
import qualified Control.Monad.Primitive as Prim

import qualified Data.Vinyl as V
import qualified Data.Vinyl.TypeLevel as V
import qualified Data.Vector.Unboxed as VU
import qualified Frames as F
import qualified Frames.Melt as F
import qualified Knit.Report as K
import qualified Numeric


enrichFrameFromBinaryModel :: forall t count m g ks rs a .
                              (rs F.⊆ rs
                              , ks F.⊆ rs
                              , FSI.RecVec rs
                              , FSI.RecVec (t ': rs)
                              , V.KnownField t
                              , V.Snd t ~ a
                              , V.KnownField count
                              , F.ElemOf rs count
                              , Integral (V.Snd count)
                              , MonadThrow m
                              , Prim.PrimMonad m
                              , F.ElemOf rs DT.PopPerSqMile
                              , Ord g
                              , Show g
                              , Ord (F.Record ks)
                              , Show (F.Record rs)
                              , Ord a
                              )
                           => DM.ModelResult g ks
                           -> (F.Record rs -> g)
                           -> a
                           -> a
                           -> F.FrameRec rs
                           -> m (F.FrameRec (t ': rs))
enrichFrameFromBinaryModel mr getGeo aTrue aFalse = enrichFrameFromModel @t @count smf where
  smf r = do
    let smf x = SplitModelF $ \n ->  let nTrue = round (realToFrac n * x) in M.fromList [(aTrue, nTrue), (aFalse, n - nTrue)]
    pTrue <- DM.applyModelResult mr (getGeo r) r
    pure $ smf pTrue

-- | produce a map of splits across type a. Doubles must sum to 1.
newtype  SplitModelF n a = SplitModelF { splitModelF :: n -> Map a n }

splitRec :: forall t count rs a . (V.KnownField t
                                  , V.Snd t ~ a
                                  , V.KnownField count
                                  , F.ElemOf rs count
                                  )
         => SplitModelF (V.Snd count) a -> F.Record rs -> [F.Record (t ': rs)]
splitRec (SplitModelF f) r =
  let makeOne (a, n) = a F.&: (F.rputField @count n) r
      splits = M.toList $ f $ F.rgetField @count r
  in makeOne <$> splits

enrichFromModel :: forall t count ks rs a . (ks F.⊆ rs
                                            , V.KnownField t
                                            , V.Snd t ~ a
                                            , V.KnownField count
                                            , F.ElemOf rs count
                                            )
                      => (F.Record ks -> Either Text (SplitModelF (V.Snd count) a)) -- ^ model results to apply
                      -> F.Record rs -- ^ record to enrich
                      -> Either Text [F.Record (t ': rs)]
enrichFromModel modelResultF recordToEnrich = flip (splitRec @t @count) recordToEnrich <$> modelResultF (F.rcast recordToEnrich)

data ModelLookupException = ModelLookupException Text deriving stock (Show)
instance Exception ModelLookupException


-- this should build the new frame in-core and in a streaming manner,
-- not needing to build the entire list before placing in-core
enrichFrameFromModel :: forall t count ks rs a m . (ks F.⊆ rs
                                                   , FSI.RecVec rs
                                                   , FSI.RecVec (t ': rs)
                                                   , V.KnownField t
                                                   , V.Snd t ~ a
                                                   , V.KnownField count
                                                   , F.ElemOf rs count
                                                   , MonadThrow m
                                                   , Prim.PrimMonad m
                                                   )
                     => (F.Record ks -> Either Text (SplitModelF (V.Snd count) a)) -- ^ model results to apply
                     -> F.FrameRec rs -- ^ record to enrich
                     -> m (F.FrameRec (t ': rs))
enrichFrameFromModel modelResultF =  FST.transform f where
  f = Streamly.concatMapM g
  g r = do
    case enrichFromModel @t @count modelResultF r of
      Left err -> throwM $ ModelLookupException err
      Right rs -> pure $ Streamly.fromList rs
