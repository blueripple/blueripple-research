{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeOperators #-}

module BlueRipple.Data.ElectionTypes where

import qualified BlueRipple.Data.DataFrames as BR
import qualified Data.Array as A
import Data.Discrimination (Grouping)
import qualified Data.Serialize as S
import qualified Data.Serialize as SE
import qualified Data.Text as T
import qualified Data.Vector.Unboxed as UVec
import Data.Vector.Unboxed.Deriving (derivingUnbox)
import qualified Data.Vinyl as V
import Data.Word (Word8)
import qualified Frames as F
import qualified Frames.InCore as FI
import qualified Frames.ShowCSV as FCSV
import qualified Frames.Visualization.VegaLite.Data as FV
import GHC.Generics (Generic)
import qualified Graphics.Vega.VegaLite as GV

-- Serialize for caching
-- FI.VectorFor for frames
-- Grouping for leftJoin

data MajorPartyParticipation
  = Neither
  | JustR
  | JustD
  | Both
  deriving (Show, Enum, Eq, Ord, Bounded, Generic)

instance S.Serialize MajorPartyParticipation

instance FCSV.ShowCSV MajorPartyParticipation

instance Grouping MajorPartyParticipation

derivingUnbox
  "MajorPartyParticipation"
  [t|MajorPartyParticipation -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

type instance FI.VectorFor MajorPartyParticipation = UVec.Vector

updateMajorPartyParticipation ::
  MajorPartyParticipation -> T.Text -> MajorPartyParticipation
updateMajorPartyParticipation Neither "republican" = JustR
updateMajorPartyParticipation Neither "democrat" = JustD
updateMajorPartyParticipation JustR "democrat" = Both
updateMajorPartyParticipation JustD "republican" = Both
updateMajorPartyParticipation x _ = x

type MajorPartyParticipationC = "MajorPartyParticipation" F.:-> MajorPartyParticipation

data PartyT = Democratic | Republican | Other deriving (Show, Enum, Bounded, Eq, Ord, Generic)

instance S.Serialize PartyT

instance Grouping PartyT

instance FCSV.ShowCSV PartyT

derivingUnbox
  "PartyT"
  [t|PartyT -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

type instance FI.VectorFor PartyT = UVec.Vector

type Party = "Party" F.:-> PartyT

instance FV.ToVLDataValue (F.ElField Party) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

data OfficeT = House | Senate | President deriving (Show, Enum, Bounded, Eq, Ord, Generic)

instance S.Serialize OfficeT

instance Grouping OfficeT

instance FCSV.ShowCSV OfficeT

derivingUnbox
  "OfficeT"
  [t|OfficeT -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

type instance FI.VectorFor OfficeT = UVec.Vector

type Office = "Office" F.:-> OfficeT

instance FV.ToVLDataValue (F.ElField Office) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

type Votes = "Votes" F.:-> Int

instance FV.ToVLDataValue (F.ElField Votes) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

type TotalVotes = "TotalVotes" F.:-> Int

instance FV.ToVLDataValue (F.ElField TotalVotes) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

data PrefTypeT = VoteShare | Inferred | PSByVoted | PSByVAP deriving (Enum, Bounded, Eq, Ord, Show, Generic)

type PrefType = "PrefType" F.:-> PrefTypeT

instance Grouping PrefTypeT

instance SE.Serialize PrefTypeT

instance FCSV.ShowCSV PrefTypeT

derivingUnbox
  "PrefTypeT"
  [t|PrefTypeT -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

type instance FI.VectorFor PrefTypeT = UVec.Vector

instance FV.ToVLDataValue (F.ElField PrefType) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

type ElectoralWeight = "ElectoralWeight" F.:-> Double

instance FV.ToVLDataValue (F.ElField ElectoralWeight) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

data ElectoralWeightSourceT = EW_Census | EW_CCES | EW_Other deriving (Enum, Bounded, Eq, Ord, Show, Generic)

type ElectoralWeightSource = "ElectoralWeightSource" F.:-> ElectoralWeightSourceT

instance Grouping ElectoralWeightSourceT

instance SE.Serialize ElectoralWeightSourceT

instance FCSV.ShowCSV ElectoralWeightSourceT

derivingUnbox
  "ElectoralWeightSourceT"
  [t|ElectoralWeightSourceT -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

type instance FI.VectorFor ElectoralWeightSourceT = UVec.Vector

instance FV.ToVLDataValue (F.ElField ElectoralWeightSource) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

data ElectoralWeightOfT
  = -- | Voting Eligible Population
    EW_Eligible
  | -- | Voting Age Population (citizens only)
    EW_Citizen
  | -- | Voting Age Population (all)
    EW_All
  deriving (Enum, Bounded, Eq, Ord, Show, Generic)

type ElectoralWeightOf = "ElectoralWeightOf" F.:-> ElectoralWeightOfT

instance Grouping ElectoralWeightOfT

instance SE.Serialize ElectoralWeightOfT

instance FCSV.ShowCSV ElectoralWeightOfT

derivingUnbox
  "ElectoralWeightOfT"
  [t|ElectoralWeightOfT -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

type instance FI.VectorFor ElectoralWeightOfT = UVec.Vector

type EWCols = [ElectoralWeightSource, ElectoralWeightOf, ElectoralWeight]

ewRec :: ElectoralWeightSourceT -> ElectoralWeightOfT -> Double -> F.Record EWCols
ewRec ws wo w = ws F.&: wo F.&: w F.&: V.RNil

type CVAP = "CVAP" F.:-> Int

instance FV.ToVLDataValue (F.ElField CVAP) where
  toVLDataValue x = (T.pack $ V.getLabel x, GV.Str $ T.pack $ show $ V.getField x)

data VoteWhyNot
  = VWN_PhysicallyUnable
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

instance Grouping VoteWhyNot

instance SE.Serialize VoteWhyNot

instance FCSV.ShowCSV VoteWhyNot

derivingUnbox
  "VoteWhyNot"
  [t|VoteWhyNot -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

type instance FI.VectorFor VoteWhyNot = UVec.Vector

data RegWhyNot
  = RWN_MissedDeadline
  | RWN_DidntKnowHow
  | RWN_Residency
  | RWN_PhysicallyUnable
  | RWN_Language
  | RWN_NotInterested
  | RWN_MyVoteIrrelevant
  | RWN_Other
  deriving (Enum, Bounded, Eq, Ord, Show, Generic)

type RegWhyNotC = "RegWhyNot" F.:-> RegWhyNot

instance Grouping RegWhyNot

instance SE.Serialize RegWhyNot

derivingUnbox
  "RegWhyNot"
  [t|RegWhyNot -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

type instance FI.VectorFor RegWhyNot = UVec.Vector

data VoteHow
  = VH_InPerson
  | VH_ByMail
  | VH_Other
  deriving (Enum, Bounded, Eq, Ord, Show, Generic)

type VoteHowC = "VoteHow" F.:-> VoteHow

instance Grouping VoteHow

instance SE.Serialize VoteHow

instance FCSV.ShowCSV VoteHow

derivingUnbox
  "VoteHow"
  [t|VoteHow -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

type instance FI.VectorFor VoteHow = UVec.Vector

data VoteWhen
  = VW_ElectionDay
  | VW_BeforeElectionDay
  | VW_Other
  deriving (Enum, Bounded, Eq, Ord, Show, Generic)

type VoteWhenC = "VoteWhen" F.:-> VoteWhen

instance Grouping VoteWhen

instance SE.Serialize VoteWhen

derivingUnbox
  "VoteWhen"
  [t|VoteWhen -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

type instance FI.VectorFor VoteWhen = UVec.Vector

data VotedYN
  = VYN_DidNotVote
  | VYN_Voted
  | VYN_Refused
  | VYN_DontKnow
  | VYN_NoResponse
  | VYN_NotInUniverse
  deriving (Enum, Bounded, Eq, Ord, Show, Generic)

type VotedYNC = "Voted" F.:-> VotedYN

instance Grouping VotedYN

instance SE.Serialize VotedYN

instance FCSV.ShowCSV VotedYN

derivingUnbox
  "VotedYN"
  [t|VotedYN -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

type instance FI.VectorFor VotedYN = UVec.Vector

data RegisteredYN
  = RYN_NotRegistered
  | RYN_Registered
  | RYN_Other
  deriving (Enum, Bounded, Eq, Ord, Show, Generic)

type RegisteredYNC = "RegisteredYN" F.:-> RegisteredYN

instance Grouping RegisteredYN

instance SE.Serialize RegisteredYN

instance FCSV.ShowCSV RegisteredYN

derivingUnbox
  "RegisteredYN"
  [t|RegisteredYN -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

type instance FI.VectorFor RegisteredYN = UVec.Vector

type DemPref = "DemPref" F.:-> Double

type DemShare = "DemShare" F.:-> Double

type DemVPV = "DemVPV" F.:-> Double

type Incumbent = "Incumbent" F.:-> Bool
