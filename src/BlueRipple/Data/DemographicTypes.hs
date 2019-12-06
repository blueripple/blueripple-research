{-# LANGUAGE DeriveGeneric     #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies      #-}
module BlueRipple.Data.DemographicTypes where

import qualified Data.Array                    as A
import qualified Data.Text                     as T
import qualified Data.Serialize                as S
import qualified Frames.InCore                 as FI
import qualified Data.Vector                   as V
import           GHC.Generics                   ( Generic )


data Sex = Female | Male deriving (Enum, Bounded, Eq, Ord, A.Ix, Show, Generic)
instance S.Serialize Sex
type instance FI.VectorFor Sex = V.Vector

data SimpleRace = NonWhite | White deriving (Eq, Ord, Enum, Bounded, A.Ix, Show, Generic)
instance S.Serialize SimpleRace
type instance FI.VectorFor SimpleRace = V.Vector

data SimpleEducation = NonGrad | Grad deriving (Eq, Ord, Enum, Bounded, A.Ix, Show, Generic)
instance S.Serialize SimpleEducation
type instance FI.VectorFor SimpleEducation = V.Vector

data SimpleAge = Old | Young deriving (Eq, Ord, Enum, Bounded, A.Ix, Show, Generic)
instance S.Serialize SimpleAge
type instance FI.VectorFor SimpleAge = V.Vector


