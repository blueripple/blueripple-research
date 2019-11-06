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

module MRP.CCESFrame
  ( module MRP.CCESPath
  , module MRP.CCESFrame
  )
where

import           MRP.CCESPath
import qualified Knit.Report                   as K

import qualified Control.Foldl                 as FL
import           Control.Monad.IO.Class         ( MonadIO(liftIO) )
import qualified Data.List                     as L
import           Data.Maybe                     ( catMaybes )
import           Data.Proxy                     ( Proxy(..) )
import qualified Data.Text                     as T
import           Data.Text                      ( Text )
import qualified Data.Vinyl                    as V
import qualified Frames                        as F
import qualified Frames.CSV                    as F
import qualified Frames.InCore                 as FI
import qualified Frames.TH                     as F

import qualified Pipes                         as P
import qualified Pipes.Prelude                 as P

import qualified Frames.ParseableTypes         as FP
import qualified Frames.MaybeUtils             as FM

-- pre-declare cols with non-standard types
F.declareColumn "Date" ''FP.FrameDay


--these columns are parsed wrong so we fix them before parsing
--F.declareColumn "CCESVvRegstatus" ''Int
F.declareColumn "CCESHispanic"    ''Int
F.declareColumn "CCESVvTurnoutGvm" ''Int
F.declareColumn "CCESVotedRepParty" ''Int
F.tableTypes' ccesRowGen

