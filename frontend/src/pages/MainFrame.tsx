import React from "react";
import "./MainFrame.less";

const MainFrame:React.FC<any> = props => {

  const HeaderPanel = () => {
    return (
      <div className="header-panel">
        <div>AI Writer</div>
      </div>
    );
  }

  return (
    <div>
      <HeaderPanel />
    </div>
  );
}

export default MainFrame;