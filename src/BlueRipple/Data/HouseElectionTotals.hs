{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC  -O0             #-}
module BlueRipple.Data.HouseElectionTotals
  (
    module BlueRipple.Data.HouseElectionTotals
  )
where

import Prelude hiding (State)
import           BlueRipple.Data.DataFrames

import qualified Control.Foldl                 as FL
import qualified Frames                        as F
import qualified Frames.Transform              as FT

import qualified Frames.MapReduce              as FMR
import qualified Frames.Folds                  as FF



type VoteTotalsByCD = [Year, StateName, StateAbbreviation, StateFips, CongressionalDistrict, Totalvotes]
type VoteTotalsByState = [Year, StateName, StateAbbreviation, StateFips, Totalvotes]

cdVoteTotalsFromHouseElectionResultsF
  :: FL.Fold HouseElections (F.FrameRec VoteTotalsByCD)
cdVoteTotalsFromHouseElectionResultsF
  = let
      preprocess =
        FT.retypeColumn @State @StateName
          . FT.retypeColumn @District @CongressionalDistrict
          . FT.retypeColumn @StatePo @StateAbbreviation
      unpack = FMR.Unpack (pure @[] . preprocess)
      assign =
        FMR.assignKeysAndData
          @'[Year, StateName, StateAbbreviation, StateFips, CongressionalDistrict]
          @'[Totalvotes]
      reduce = FMR.foldAndAddKey
        $ FF.foldAllConstrained @Num (fmap (fromMaybe 0) FL.last)
    in
      FMR.concatFold $ FMR.mapReduceFold unpack assign reduce


stateVoteTotalsFromCDVoteTotalsF
  :: FL.Fold (F.Record VoteTotalsByCD) (F.FrameRec VoteTotalsByState)
stateVoteTotalsFromCDVoteTotalsF =
  let unpack = FMR.noUnpack
      assign =
        FMR.assignKeysAndData @'[Year, StateName, StateAbbreviation, StateFips]
          @'[Totalvotes]
      reduce = FMR.foldAndAddKey $ FF.foldAllConstrained @Num FL.sum
  in  FMR.concatFold $ FMR.mapReduceFold unpack assign reduce


-- is there a better way to merge these folds??
stateVoteTotalsFromHouseElectionResultsF
  :: FL.Fold HouseElections (F.FrameRec VoteTotalsByState)
stateVoteTotalsFromHouseElectionResultsF = fmap
  (FL.fold stateVoteTotalsFromCDVoteTotalsF)
  cdVoteTotalsFromHouseElectionResultsF
