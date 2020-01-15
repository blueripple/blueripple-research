{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}
{-# LANGUAGE TypeOperators     #-}
module BlueRipple.Data.ElectionTypes where

import qualified Data.Array                    as A
import qualified Data.Text                     as T
import qualified Data.Serialize                as S
import qualified Frames                        as F
import qualified Frames.InCore                 as FI
import qualified Data.Vector                   as V
import           GHC.Generics                   ( Generic )
import           Data.Discrimination            ( Grouping )
import           BlueRipple.Data.DataFrames
-- Serialize for caching
-- FI.VectorFor for frames
-- Grouping for leftJoin

data MajorPartyParticipation = Neither
                             | JustR
                             | JustD
                             | Both deriving (Show,Enum,Eq,Ord,Bounded,Generic)

instance S.Serialize MajorPartyParticipation
type instance FI.VectorFor MajorPartyParticipation = V.Vector
instance Grouping MajorPartyParticipation

updateMajorPartyParticipation
  :: MajorPartyParticipation -> T.Text -> MajorPartyParticipation
updateMajorPartyParticipation Neither "republican" = JustR
updateMajorPartyParticipation Neither "democrat"   = JustD
updateMajorPartyParticipation JustR   "democrat"   = Both
updateMajorPartyParticipation JustD   "republican" = Both
updateMajorPartyParticipation x       _            = x

type MajorPartyParticipationC = "MajorPartyParticipation" F.:-> MajorPartyParticipation


