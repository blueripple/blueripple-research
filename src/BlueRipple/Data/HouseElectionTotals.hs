{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# OPTIONS_GHC  -O0             #-}
module BlueRipple.Data.HouseElectionTotals where

import           BlueRipple.Data.DataFrames
import qualified Knit.Report                   as K

import qualified Control.Foldl                 as FL
import qualified Data.List                     as L
import           Data.Maybe                     ( catMaybes
                                                , fromMaybe
                                                )
import qualified Data.Text                     as T
import           Data.Text                      ( Text )
import qualified Data.Vinyl                    as V
import qualified Frames                        as F
import qualified Frames.CSV                    as F
import qualified Frames.InCore                 as FI
import qualified Frames.TH                     as F

import qualified Frames.ParseableTypes         as FP
import qualified Frames.MaybeUtils             as FM
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
