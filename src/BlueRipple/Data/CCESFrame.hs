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
import qualified Knit.Report                   as K

import qualified Control.Foldl                 as FL
import           Control.Monad.IO.Class         ( MonadIO(liftIO) )
import qualified Data.List                     as L
import           Data.Maybe                     ( catMaybes )
import           Data.Proxy                     ( Proxy(..) )
import qualified Data.Serialize                as S
import qualified Data.Text                     as T
import           Data.Text                      ( Text )
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Data.Vector.Unboxed as UVec
import Data.Vector.Unboxed.Deriving (derivingUnbox)
import qualified Frames                        as F
import qualified Frames.InCore                 as FI
--import qualified Frames.Streamly.CSV                    as F
import qualified Frames.Streamly.TH                     as FS
import qualified Flat

import qualified Pipes                         as P
import qualified Pipes.Prelude                 as P

import qualified Frames.ParseableTypes         as FP
import qualified Frames.MaybeUtils             as FM
import qualified Relude.Extra as Relude

-- pre-declare cols with non-standard types
F.declareColumn "Date" ''FP.FrameDay

--these columns are parsed wrong so we fix them before parsing
FS.declarePrefixedColumn "hispanic" "CCES" ''Int

--FS.tableTypes' ccesRowGen2018
FS.tableTypes' ccesRowGen2020C
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
                          | CR_Missing deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor CatalistRegistration = UVec.Vector
instance S.Serialize CatalistRegistration
instance Flat.Flat CatalistRegistration

derivingUnbox
  "CatalistRegistration"
  [t|CatalistRegistration -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

type CatalistRegistrationC = "CatalistRegistration" F.:-> CatalistRegistration

cesIntToRegistration :: Int -> CatalistRegistration
cesIntToRegistration = fromMaybe CR_Missing . Relude.safeToEnum . minus1

data CatalistTurnout = CT_Absentee
                     | CT_Early
                     | CT_Mail
                     | CT_Polling
                     | CT_Unknown
                     | CT_Missing deriving (Show, Enum, Bounded, Eq, Ord, Generic)
type instance FI.VectorFor CatalistTurnout = UVec.Vector
instance S.Serialize CatalistTurnout
instance Flat.Flat CatalistTurnout

derivingUnbox
  "CatalistTurnout"
  [t|CatalistTurnout -> Word8|]
  [|toEnum . fromEnum|]
  [|toEnum . fromEnum|]

type CatalistTurnoutC = "CatalistTurnout" F.:-> CatalistTurnout

cesIntToTurnout :: Int -> CatalistTurnout
cesIntToTurnout = fromMaybe CT_Missing . Relude.safeToEnum . minus1
