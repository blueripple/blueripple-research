{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE DeriveGeneric       #-}
{-# LANGUAGE GADTs               #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE FlexibleInstances    #-}
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
import qualified Data.Text                     as T
import           Data.Text                      ( Text )
import qualified Data.Vinyl                    as V
import qualified Data.Vinyl.TypeLevel          as V
import qualified Frames                        as F
--import qualified Frames.Streamly.CSV                    as F
import qualified Frames.Streamly.TH                     as FS

import qualified Pipes                         as P
import qualified Pipes.Prelude                 as P

import qualified Frames.ParseableTypes         as FP
import qualified Frames.MaybeUtils             as FM
--import GHC.IO.FD (FD(fdIsNonBlocking))

-- pre-declare cols with non-standard types
F.declareColumn "Date" ''FP.FrameDay


--these columns are parsed wrong so we fix them before parsing
FS.declarePrefixedColumn "hispanic" "CCES" ''Int

--FS.tableTypes' ccesRowGen2018
FS.tableTypes' ccesRowGen2020
