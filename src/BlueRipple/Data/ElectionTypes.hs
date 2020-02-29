{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE TypeOperators     #-}
module BlueRipple.Data.ElectionTypes where

import qualified Data.Array                    as A
import qualified Data.Text                     as T
import qualified Data.Serialize                as S
import qualified Frames                        as F
import qualified Frames.InCore                 as FI
import qualified Data.Serialize                as SE
import qualified Data.Vinyl                    as V
import qualified Data.Vector                   as Vec
import           GHC.Generics                   ( Generic )
import           Data.Discrimination            ( Grouping )
import qualified Graphics.Vega.VegaLite        as GV
import qualified Frames.Visualization.VegaLite.Data
                                               as FV
import qualified BlueRipple.Data.DataFrames    as BR

-- Serialize for caching
-- FI.VectorFor for frames
-- Grouping for leftJoin

data MajorPartyParticipation = Neither
                             | JustR
                             | JustD
                             | Both deriving (Show,Enum,Eq,Ord,Bounded,Generic)

instance S.Serialize MajorPartyParticipation
type instance FI.VectorFor MajorPartyParticipation = Vec.Vector
instance Grouping MajorPartyParticipation

updateMajorPartyParticipation
  :: MajorPartyParticipation -> T.Text -> MajorPartyParticipation
updateMajorPartyParticipation Neither "republican" = JustR
updateMajorPartyParticipation Neither "democrat"   = JustD
updateMajorPartyParticipation JustR   "democrat"   = Both
updateMajorPartyParticipation JustD   "republican" = Both
updateMajorPartyParticipation x       _            = x

type MajorPartyParticipationC = "MajorPartyParticipation" F.:-> MajorPartyParticipation

data PartyT = Democratic | Republican | Other deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor PartyT = Vec.Vector
instance S.Serialize PartyT
instance Grouping PartyT

type Party = "Party" F.:-> PartyT
instance FV.ToVLDataValue (F.ElField Party) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

data OfficeT = House | Senate | President deriving (Show,  Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor OfficeT = Vec.Vector
instance S.Serialize OfficeT
instance Grouping OfficeT

type Office = "Office" F.:-> OfficeT
instance FV.ToVLDataValue (F.ElField Office) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

type Votes = "Votes" F.:-> Int
instance FV.ToVLDataValue (F.ElField Votes) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

type TotalVotes = "TotalVotes" F.:-> Int
instance FV.ToVLDataValue (F.ElField TotalVotes) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

data PrefTypeT = VoteShare | Inferred | PSByVoted | PSByVAP deriving (Enum, Bounded, Eq , Ord, Show, Generic)

type PrefType = "PrefType" F.:-> PrefTypeT
type instance FI.VectorFor PrefTypeT = Vec.Vector
instance Grouping PrefTypeT
instance SE.Serialize PrefTypeT

instance FV.ToVLDataValue (F.ElField PrefType) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

type ElectoralWeight = "ElectoralWeight" F.:-> Double
instance FV.ToVLDataValue (F.ElField ElectoralWeight) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

data ElectoralWeightSourceT = EW_Census | EW_CCES | EW_Other deriving (Enum, Bounded, Eq , Ord, Show, Generic)
type ElectoralWeightSource = "ElectoralWeightSource" F.:-> ElectoralWeightSourceT
type instance FI.VectorFor ElectoralWeightSourceT = Vec.Vector
instance Grouping ElectoralWeightSourceT
instance SE.Serialize ElectoralWeightSourceT

data ElectoralWeightOfT = EW_Eligible -- ^ Voting Eligible Population
                        | EW_Citizen -- ^ Voting Age Population (citizens only)
                        | EW_All -- ^ Voting Age Population (all)
                        deriving (Enum, Bounded, Eq , Ord, Show, Generic)
type ElectoralWeightOf = "ElectoralWeightOf" F.:-> ElectoralWeightOfT
type instance FI.VectorFor ElectoralWeightOfT = Vec.Vector
instance Grouping ElectoralWeightOfT
instance SE.Serialize ElectoralWeightOfT


