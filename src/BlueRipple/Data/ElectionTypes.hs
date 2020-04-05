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

type EWCols = [ElectoralWeightSource, ElectoralWeightOf, ElectoralWeight]

ewRec :: ElectoralWeightSourceT -> ElectoralWeightOfT -> Double -> F.Record EWCols
ewRec ws wo w = ws F.&: wo F.&: w F.&: V.RNil

type ElectoralWeight = "ElectoralWeight" F.:-> Double
instance FV.ToVLDataValue (F.ElField ElectoralWeight) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

data ElectoralWeightSourceT = EW_Census | EW_CCES | EW_Other deriving (Enum, Bounded, Eq , Ord, Show, Generic)
type ElectoralWeightSource = "ElectoralWeightSource" F.:-> ElectoralWeightSourceT
type instance FI.VectorFor ElectoralWeightSourceT = Vec.Vector
instance Grouping ElectoralWeightSourceT
instance SE.Serialize ElectoralWeightSourceT
instance FV.ToVLDataValue (F.ElField ElectoralWeightSource) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)


data ElectoralWeightOfT = EW_Eligible -- ^ Voting Eligible Population
                        | EW_Citizen -- ^ Voting Age Population (citizens only)
                        | EW_All -- ^ Voting Age Population (all)
                        deriving (Enum, Bounded, Eq , Ord, Show, Generic)
type ElectoralWeightOf = "ElectoralWeightOf" F.:-> ElectoralWeightOfT
type instance FI.VectorFor ElectoralWeightOfT = Vec.Vector
instance Grouping ElectoralWeightOfT
instance SE.Serialize ElectoralWeightOfT


type CVAP = "CVAP" F.:-> Int
instance FV.ToVLDataValue (F.ElField CVAP) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)


data VoteWhyNot = VWN_PhysicallyUnable
                | VWN_Away
                | VWN_Forgot
                | VWN_NotInterested
                | VWN_Busy
                | VWN_Transport
                | VWN_DislikeChoices
                | VWN_RegIssue
                | VWN_Weather
                | VWN_BadPollingPlace
                | VWN_Other
                deriving (Enum, Bounded, Eq, Ord, Show, Generic)

type VoteWhyNotC = "VoteWhyNot" F.:-> VoteWhyNot
type instance FI.VectorFor VoteWhyNot = Vec.Vector
instance Grouping VoteWhyNot
instance SE.Serialize VoteWhyNot

data RegWhyNot = RWN_MissedDeadline
               | RWN_DidntKNowHow
               | RWN_Residency
               | RWN_PhysicallyUnable
               | RWN_Language
               | RWN_NotInterested
               | RWN_MyVoteIrrelevant
               | RWN_Other
                deriving (Enum, Bounded, Eq, Ord, Show, Generic)

type RegWhyNotC = "RegWhyNot" F.:-> RegWhyNot
type instance FI.VectorFor RegWhyNot = Vec.Vector
instance Grouping RegWhyNot
instance SE.Serialize RegWhyNot

data VoteHow = VH_InPerson
             | VH_ByMail
             | VH_Other
             deriving (Enum, Bounded, Eq, Ord, Show, Generic)


type VoteHowC = "VoteHow" F.:-> VoteHow
type instance FI.VectorFor VoteHow = Vec.Vector
instance Grouping VoteHow
instance SE.Serialize VoteHow

data VoteWhen = VW_ElectionDay
              | VW_BeforeElectionDay
              | VW_Other              
              deriving (Enum, Bounded, Eq, Ord, Show, Generic)

type VoteWhenC = "VoteWhen" F.:-> VoteWhen
type instance FI.VectorFor VoteWhen = Vec.Vector
instance Grouping VoteWhen
instance SE.Serialize VoteWhen

data Voted = V_Not
           | V_Voted
           | V_Other
           deriving (Enum, Bounded, Eq, Ord, Show, Generic)

type VotedC = "Voted" F.:-> Voted
type instance FI.VectorFor Voted = Vec.Vector
instance Grouping Voted
instance SE.Serialize Voted

type Registered = R_Not
                | R_Registered
                | R_Other
                deriving (Enum, Bounded, Eq, Ord, Show, Generic)
           
type RegisteredC = "Voted" F.:-> Registered
type instance FI.VectorFor Registered = Vec.Vector
instance Grouping Registered
instance SE.Serialize Registered



