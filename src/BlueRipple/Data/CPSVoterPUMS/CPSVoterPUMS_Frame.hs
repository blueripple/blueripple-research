{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE TypeApplications #-}
module BlueRipple.Data.CPSVoterPUMS.CPSVoterPUMS_Frame
  (
    module BlueRipple.Data.CPSVoterPUMS.CPSVoterPUMS_Path
  , module BlueRipple.Data.CPSVoterPUMS.CPSVoterPUMS_Frame
  ) where

import           BlueRipple.Data.CPSVoterPUMS.CPSVoterPUMS_Path

import qualified Frames.Streamly.TH                     as F

F.tableTypes' cpsVoterPUMSRowGen
