{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE PolyKinds           #-}
{-# LANGUAGE QuasiQuotes         #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeFamilies        #-}
{-# LANGUAGE TypeOperators       #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE Rank2Types          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE UndecidableInstances #-}

module BlueRipple.Data.CCESFrame
  ( module BlueRipple.Data.CCESPath
  , module BlueRipple.Data.CCESFrame
  )
where

import           BlueRipple.Data.CCESPath
import qualified Data.Serialize                as S
import qualified Data.Text                     as T
import qualified Data.Vector.Unboxed as UVec
import Data.Vector.Unboxed.Deriving (derivingUnbox)
import qualified Frames                        as F
import qualified Frames.Streamly.InCore                 as FI
import qualified Frames.Streamly.TH                     as FS
import qualified Flat

import qualified Frames.ParseableTypes         as FP
import qualified Relude.Extra as Relude

-- pre-declare cols with non-standard types
F.declareColumn "Date" ''FP.FrameDay

--these columns are parsed wrong so we fix them before parsing
FS.declarePrefixedColumn "hispanic" "CCES" ''Int
FS.declarePrefixedColumn "VVoterStatus" "CES" ''Int

--FS.tableTypes' ccesRowGen2018
FS.tableTypes' ccesRowGen2020C
FS.tableTypes' cesRowGen2022
FS.tableTypes' cesRowGen2020
FS.tableTypes' cesRowGen2018
FS.tableTypes' cesRowGen2016

-- extra types for CES
minus1 :: Num a => a -> a
minus1 x = x - 1
{-# INLINE minus1 #-}

data VRegistration = VR_Active
                   | VR_Dropped
                   | VR_Inactive
                   | VR_Multiple
                   | VR_UnRegistered
                   | VR_Missing deriving stock (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor VRegistration = UVec.Vector
instance S.Serialize VRegistration
instance Flat.Flat VRegistration

derivingUnbox
  "VRegistration"
  [t|VRegistration -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

F.declareColumn "VRegistrationC" ''VRegistration

catalistRegistrationFromNText :: Int -> Text -> VRegistration
catalistRegistrationFromNText n t
  | f t == f "active" = VR_Active
  | f t == f "dropped" = VR_Dropped
  | f t == f "inactive" = VR_Inactive
  | f t == f "multiple" = VR_Multiple
  | f t == f "unregistered" = VR_UnRegistered
  | otherwise = VR_Missing
  where
    f = T.take n

cesIntToRegistration :: Int -> VRegistration
cesIntToRegistration = fromMaybe VR_Missing . Relude.safeToEnum . minus1

tsIntToRegistration :: Int -> VRegistration
tsIntToRegistration 1 = VR_Active
tsIntToRegistration _ = VR_UnRegistered


registered :: VRegistration -> Bool
registered VR_Active = True
registered _ = False

data VTurnout = VT_Absentee
             | VT_Early
             | VT_Mail
             | VT_Polling
             | VT_Unknown
             | VT_Missing deriving stock (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor VTurnout = UVec.Vector
instance S.Serialize VTurnout
instance Flat.Flat VTurnout

derivingUnbox
  "VTurnout"
  [t|VTurnout -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

F.declareColumn "VTurnoutC" ''VTurnout

catalistTurnoutFromNText :: Int -> Text -> VTurnout
catalistTurnoutFromNText n t
  | f t == f "absentee" = VT_Absentee
  | f t == f "earlyVote" = VT_Early
  | f t == f "mail" = VT_Mail
  | f t == f "polling" = VT_Polling
  | f t == f "unknown" = VT_Unknown
  | otherwise = VT_Missing
  where
    f = T.take n

cesIntToTurnout :: Int -> VTurnout
cesIntToTurnout = fromMaybe VT_Missing . Relude.safeToEnum . minus1

tsIntToTurnout :: Int -> VTurnout
tsIntToTurnout n | n == 1 = VT_Absentee
                 | n == 2 = VT_Early
                 | n == 3 = VT_Mail
                 | n == 4 = VT_Polling
                 | n == 5 = VT_Polling -- this is actually provisional but we have no field for that
                 | n == 6 = VT_Unknown
                 | otherwise = VT_Missing -- this is also bad since missing is not the same as did not vote

voted :: VTurnout -> Bool
voted VT_Missing = False
voted _ = True
