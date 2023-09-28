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

--FS.tableTypes' ccesRowGen2018
FS.tableTypes' ccesRowGen2020C
--FS.tableTypes' cesRowGen2022
FS.tableTypes' cesRowGen2020
FS.tableTypes' cesRowGen2018
FS.tableTypes' cesRowGen2016

-- extra types for CES
minus1 :: Num a => a -> a
minus1 x = x - 1
{-# INLINE minus1 #-}

data CatalistRegistration = CR_Active
                          | CR_Dropped
                          | CR_Inactive
                          | CR_Multiple
                          | CR_UnRegistered
                          | CR_Missing deriving stock (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor CatalistRegistration = UVec.Vector
instance S.Serialize CatalistRegistration
instance Flat.Flat CatalistRegistration

derivingUnbox
  "CatalistRegistration"
  [t|CatalistRegistration -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

F.declareColumn "CatalistRegistrationC" ''CatalistRegistration

catalistRegistrationFromNText :: Int -> Text -> CatalistRegistration
catalistRegistrationFromNText n t
  | f t == f "active" = CR_Active
  | f t == f "dropped" = CR_Dropped
  | f t == f "inactive" = CR_Inactive
  | f t == f "multiple" = CR_Multiple
  | f t == f "unregistered" = CR_UnRegistered
  | otherwise = CR_Missing
  where
    f = T.take n

cesIntToRegistration :: Int -> CatalistRegistration
cesIntToRegistration = fromMaybe CR_Missing . Relude.safeToEnum . minus1

tsBoolToRegistration :: Bool -> CatalistRegistration
tsBoolToRegistration True = CR_Active
tsBoolToRegistration True = CR_UnRegistered


catalistRegistered :: CatalistRegistration -> Bool
catalistRegistered CR_Active = True
catalistRegistered _ = False

data CatalistTurnout = CT_Absentee
                     | CT_Early
                     | CT_Mail
                     | CT_Polling
                     | CT_Unknown
                     | CT_Missing deriving stock (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor CatalistTurnout = UVec.Vector
instance S.Serialize CatalistTurnout
instance Flat.Flat CatalistTurnout

derivingUnbox
  "CatalistTurnout"
  [t|CatalistTurnout -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

F.declareColumn "CatalistTurnoutC" ''CatalistTurnout

catalistTurnoutFromNText :: Int -> Text -> CatalistTurnout
catalistTurnoutFromNText n t
  | f t == f "absentee" = CT_Absentee
  | f t == f "earlyVote" = CT_Early
  | f t == f "mail" = CT_Mail
  | f t == f "polling" = CT_Polling
  | f t == f "unknown" = CT_Unknown
  | otherwise = CT_Missing
  where
    f = T.take n

cesIntToTurnout :: Int -> CatalistTurnout
cesIntToTurnout = fromMaybe CT_Missing . Relude.safeToEnum . minus1

tsIntToTurnout :: Int -> CatalistTurnout
tsIntToTurnout n | n == 1 = CT_Absentee
                 | n == 2 = CT_Early
                 | n == 3 = CT_Mail
                 | n == 4 = CT_Polling
                 | n == 5 = CT_Polling -- this is actually provisional but we have no field for that
                 | n == 6 = CT_Unknown
                 | otherwise = CT_Missing -- this is also bad since missing is not the same as did not vote

catalistVoted :: CatalistTurnout -> Bool
catalistVoted CT_Missing = False
catalistVoted _ = True
