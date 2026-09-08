import React from 'react';

const APPROACHES = ['Stream processor', 'Custom loop', 'Spate'];
const RESPONSIBILITIES = [
  {
    name: 'Transform interface',
    values: [
      'Processor-defined APIs and operators.',
      'Functions and control flow you choose.',
      'Rust closures composed into an operator chain.',
    ],
  },
  {
    name: 'Delivery implementation',
    values: [
      'Configure the processor’s facilities; available guarantees and policies vary.',
      'Implement and test backpressure, error handling, commits and shutdown in your loop.',
      'Configure framework backpressure, Skip/Fail policies, checkpointing and drain limits.',
    ],
  },
  {
    name: 'Operating model',
    values: [
      'Depends on the processor: embedded runtime, cluster or managed service.',
      'Build, deploy and operate your application and its dependencies.',
      'Build a Rust binary; deploy and operate your pipeline and its dependencies.',
    ],
  },
];

export default function Responsibilities(): React.JSX.Element {
  return (
    <div className="home-responsibilities">
      <h3 id="responsibilities-title">Responsibilities by approach</h3>
      <p id="responsibilities-note">
        Stream processors differ in their APIs, guarantees and deployment options.
        Evaluate the specific processor and configuration.
      </p>
      <table className="home-responsibilities__table" aria-labelledby="responsibilities-title"
        aria-describedby="responsibilities-note">
        <thead>
          <tr>
            <th scope="col">Responsibility</th>
            {APPROACHES.map((name) => <th key={name} scope="col">{name}</th>)}
          </tr>
        </thead>
        <tbody>
          {RESPONSIBILITIES.map((row) => (
            <tr key={row.name}>
              <th scope="row">{row.name}</th>
              {row.values.map((value, i) => <td key={APPROACHES[i]}>{value}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
      <div className="home-responsibilities__groups">
        {RESPONSIBILITIES.map((row) => (
          <section key={row.name} className="home-responsibilities__group"
            aria-label={row.name}>
            <h4>{row.name}</h4>
            <dl>
              {row.values.map((value, i) => (
                <div key={APPROACHES[i]}>
                  <dt>{APPROACHES[i]}</dt>
                  <dd>{value}</dd>
                </div>
              ))}
            </dl>
          </section>
        ))}
      </div>
    </div>
  );
}
