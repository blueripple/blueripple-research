module BlueRipple.Data.CPSTurnoutPUMS.CPSTurnoutPUMS_Frame
  ( module BlueRipple.Data.CPSTurnoutPUMS.CPSTurnout_Path
  , module BlueRipple.Data.CPSTurnoutPUMS.CPSTurnout_Frame
  ) where

import           BlueRipple.Data.CPSVoterPUMS.CPSVoterPUMS_Path

import qualified Frames.Streamly.TH                     as F

F.tableTypes' cpsTurnoutPUMSRowGen
